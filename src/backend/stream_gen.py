import asyncio
import json
import markdown2
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from openai import OpenAI
from pydantic import BaseModel

from db_operations import (
    create_session, validate_session_and_thread_tokens, get_thread_id_from_encrypted_thread_token,
    create_thread, update_session_last_active, update_thread_last_active,
)

# LATER - Receive reminder when you have to buy back the primary security
# LATER - Marketing - What's a good way to build creatives for a loss harvesting product?


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

client = OpenAI(organization="org-vvQl4oYpM1sRl9uRk3PejmUS",
                project="proj_jAx3xZMcN9n2EVR2FhS8zsNk",
                api_key=os.getenv('OPENAI_SECRET_KEY'))

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ "http://localhost:3000", "http://localhost:5173" ],
    allow_credentials=True,
    allow_methods=[ "*" ],
    allow_headers=[ "*" ]
)


@app.get('/get_session_token')
async def get_session_token():
    try:
        encrypted_token = create_session()
        return JSONResponse(content={"session_token": encrypted_token})
    except Exception as e:
        logger.error(f"Error generating session token: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate session token")


async def create_new_thread_and_add_message(session_token, prompt):
    logger.info("Creating new thread due to rate limit")
    new_thread = client.beta.threads.create(
        messages=[ {"role": "user", "content": prompt} ]
    )
    thread_id = new_thread.id
    encrypted_thread_token = create_thread(session_token, thread_id)

    logger.info(f"New thread created with id: {thread_id}")

    return thread_id, encrypted_thread_token


def parse_ai_output(ai_output):
    try:
        output_json = json.loads(ai_output)
        response_structure = output_json[ 'ambiguity_clarification' ][ 'response' ][ 'response_structure' ]

        formatted_response = [ ]
        for section in response_structure:
            heading = section[ 'heading' ]
            content = section[ 'content' ]
            is_numbered_list = section[ 'numbered_list' ]

            if heading:
                formatted_response.append(f"## {heading}\n\n")

            if is_numbered_list:
                for i, item in enumerate(content, 1):
                    formatted_response.append(f"{i}. {item}\n")
            else:
                formatted_response.append(f"{content}\n\n")

        return ''.join(formatted_response)
    except Exception as e:
        logger.error(f"Error parsing AI output: {str(e)}")
        return f"Error: Unable to parse AI response. {str(e)}"


async def stream_generator(thread_id, assistant_id, prompt, session_token):
    try:
        logger.info(f"Creating run for thread_id: {thread_id}, assistant_id: {assistant_id}")
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )
        logger.info(f"Run created with id: {run.id}")
        while True:
            logger.info(f"Retrieving run status for run_id: {run.id}")
            run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            logger.info(f"Current run status: {run.status}")
            if run.status == 'completed':
                logger.info("Run completed, retrieving messages")
                # Retrieve messages, they are sorted with the newest first
                messages = client.beta.threads.messages.list(thread_id=thread_id)
                logger.info(f"Retrieved {len(messages.data)} messages")

                # Find the most recent assistant message
                latest_assistant_message = next((msg for msg in messages.data if msg.role == "assistant"), None)

                if latest_assistant_message:
                    logger.info("Found latest assistant message, streaming content")
                    for content in latest_assistant_message.content:
                        if content.type == 'text':
                            parsed_content = parse_ai_output(content.text.value)
                            html_content = markdown2.markdown(parsed_content)

                            # Stream each word separately
                            for word in html_content.split():
                                yield json.dumps({'type': 'message', 'content': word + ' '})
                                # yield word + ' '
                                await asyncio.sleep(0.1)  # Small delay between words
                else:
                    logger.warning("No assistant message found in the thread.")
                break
            elif run.status == 'failed':
                logger.error(f"Run failed. Reason: {run.last_error}")
                if run.last_error.code == 'rate_limit_exceeded':
                    logger.info("Rate limit exceeded. Creating new thread and retrying.")
                    new_thread_id, new_thread_token = await create_new_thread_and_add_message(session_token,
                                                                                              prompt)  # potential bug here
                    yield json.dumps({'type': 'new_thread', 'thread_token': new_thread_token})
                    async for chunk in stream_generator(new_thread_id, assistant_id, prompt, session_token):
                        yield chunk
                    return
                else:
                    yield json.dumps({'type': 'error', 'content': f"Run failed: {run.last_error.message}"})
                break
            elif run.status in [ 'cancelled', 'expired' ]:
                logger.error(f"Run ended with status: {run.status}")
                yield json.dumps({'type': 'error', 'content': f"Run {run.status}"})
                break
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in stream_generator: {str(e)}")
        yield json.dumps({'type': 'error', 'content': str(e)})


class UserMessage(BaseModel):
    prompt: str
    session_token: str | None = None
    thread_token: str | None = None


@app.options("/chat/stream")
async def options_chat_stream():
    return {"message": "OK"}


@app.post('/chat/stream')
async def stream_ai_response(response: UserMessage):
    logger.info(f"Received request: {response}")
    response_dict = response.model_dump()

    async def event_generator():
        try:
            session_token = response_dict[ "session_token" ]
            thread_token = response_dict[ "thread_token" ]
            prompt = response_dict[ "prompt" ]

            # Validate tokens
            logger.info(f"Validating tokens: session_token={session_token}, thread_token={thread_token}")
            token_status = validate_session_and_thread_tokens(session_token, thread_token)
            logger.info(f"Token status: {token_status}")

            if not token_status[ 'session' ]:
                logger.info("Creating new session and thread")
                # Create new session and thread
                session_token = create_session()
                new_thread = client.beta.threads.create(
                    messages=[ {"role": "user", "content": prompt} ]
                )
                thread_id = new_thread.id
                thread_token = create_thread(session_token, thread_id)
            elif token_status[ 'session' ] and (not token_status[ 'thread' ]):
                logger.info("Creating new thread for existing session")
                # Create new thread for existing session
                new_thread = client.beta.threads.create(
                    messages=[ {"role": "user", "content": prompt} ]
                )
                thread_id = new_thread.id
                thread_token = create_thread(session_token, thread_id)
            else:
                # Use existing thread
                logger.info("Using existing thread")
                thread_id = get_thread_id_from_encrypted_thread_token(session_token, thread_token)
                client.beta.threads.messages.create(
                    thread_id=thread_id,
                    role="user",
                    content=prompt
                )

            # Update last active timestamps
            logger.info("Updating last active timestamps")
            update_session_last_active(session_token)
            update_thread_last_active(thread_token)

            logger.info(f"Starting stream generation for thread {thread_id}")
            async for chunk in stream_generator(thread_id=thread_id, assistant_id="asst_9od8x1PjV2GbjOcriSNcl2ph",
                                                prompt=prompt, session_token=session_token):
                # if chunk is None:
                #     logger.warning("Received None chunk, breaking stream")
                #     break
                chunk_data = json.loads(chunk)
                if chunk_data[ 'type' ] in [ 'message', 'new_thread', 'error' ]:
                    yield f"data: {json.dumps({'type': 'message', 'content': chunk})}\n\n"

                logger.info("Stream generation completed")
                # Include session and thread tokens in the final response
                yield f"data: {json.dumps({'type': 'done', 'content': 'Stream finished', 'session_token': session_token, 'thread_token': thread_token})}\n\n"
        except Exception as e:
            logger.error(f"Error in event_generator: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


async def execute_run(thread_id, thread_token, session_token, prompt):
    run = client.beta.threads.runs.create(
        thread_id=thread_id,
        assistant_id="asst_9od8x1PjV2GbjOcriSNcl2ph"
    )

    # Wait for run to complete
    while run.status not in [ 'completed', 'failed', 'cancelled', 'expired' ]:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
        await asyncio.sleep(1)

    if run.status == 'completed':
        messages = client.beta.threads.messages.list(thread_id=thread_id)
        latest_assistant_message = next((msg for msg in messages.data if msg.role == "assistant"), None)

        if latest_assistant_message:
            for content in latest_assistant_message.content:
                if content.type == 'text':
                    # logger.info(f"Content type: {type(content.text.value)}")
                    # logger.info(f"Content value: {content.text.value}")

                    try:
                        ai_response = json.loads(content.text.value)
                        response_structure = ai_response[ 'ambiguity_clarification' ][ 'response' ][
                            'response_structure' ]
                    except json.JSONDecodeError:
                        # If content.text.value is not JSON, return it as is
                        response_structure = {"type": "error", "content": "text not in JSON format"}
                    except KeyError:
                        # If the expected structure is not found, return the whole response
                        response_structure = {"type": "error", "content": "Key error - JSON structure not as expected."}

                    return {
                        "response": response_structure,
                        "session_token": session_token,
                        "thread_token": thread_token
                    }
    elif run.status == 'failed':
        logger.error(f"Run failed. Reason: {run.last_error}")
        if run.last_error.code == 'rate_limit_exceeded':
            logger.info("Rate limit exceeded. Creating new thread and retrying.")
            new_thread_id, new_thread_token = await create_new_thread_and_add_message(session_token, prompt)
            return await execute_run(thread_id=new_thread_id, thread_token=new_thread_token,
                                     session_token=session_token,
                                     prompt=prompt)
        else:
            return {'type': 'error', 'content': f"Run failed: {run.last_error.message}"}
    elif run.status in [ 'cancelled', 'expired' ]:
        logger.error(f"Run ended with status: {run.status}")
        return {'type': 'error', 'content': f"Run {run.status}"}


async def session_and_token_management(session_token, thread_token, prompt):
    logger.info(f"Validating tokens: session_token={session_token}, thread_token={thread_token}")
    token_status = validate_session_and_thread_tokens(session_token, thread_token)
    logger.info(f"Token status: {token_status}")

    if not token_status[ 'session' ]:
        logger.info("Creating new session and thread")
        # Create new session and thread
        session_token = create_session()
        new_thread = client.beta.threads.create(
            messages=[ {"role": "user", "content": prompt} ]
        )
        thread_id = new_thread.id
        thread_token = create_thread(session_token, thread_id)
    elif token_status[ 'session' ] and (not token_status[ 'thread' ]):
        logger.info("Creating new thread for existing session")
        # Create new thread for existing session
        new_thread = client.beta.threads.create(
            messages=[ {"role": "user", "content": prompt} ]
        )
        thread_id = new_thread.id
        thread_token = create_thread(session_token, thread_id)
    else:
        # Use existing thread
        logger.info("Using existing thread")
        thread_id = get_thread_id_from_encrypted_thread_token(session_token, thread_token)
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=prompt
        )

    # Update last active timestamps
    logger.info("Updating last active timestamps")
    update_session_last_active(session_token)
    update_thread_last_active(thread_token)

    return {
        "session_token": session_token,
        "thread_id": thread_id,
        "thread_token": thread_token,
        "prompt": prompt
    }


@app.options("/chat")
async def options_chat():
    return {"message": "OK"}


@app.post('/chat')
async def chat_response(response: UserMessage):
    logger.info(f"Received request: {response}")
    response_dict = response.model_dump()

    try:
        validated_response_dict = await session_and_token_management(response_dict[ "session_token" ],
                                                                     response_dict[ "thread_token" ],
                                                                     response_dict[ "prompt" ])

        session_token = validated_response_dict[ "session_token" ]
        thread_token = validated_response_dict[ "thread_token" ]
        thread_id = validated_response_dict[ "thread_id" ]
        prompt = validated_response_dict[ "prompt" ]

        result = await execute_run(thread_id=thread_id, thread_token=thread_token, session_token=session_token,
                                   prompt=prompt)

        if "type" in result and result[ "type" ] == "error":
            raise HTTPException(status_code=500, detail=result[ "error" ])

        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"Error in chat_response: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
