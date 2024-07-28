import asyncio
import json
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from openai import OpenAI
from pydantic import BaseModel

from db_operations import (
    create_session, validate_session_and_thread_tokens, get_latest_thread_for_session,
    create_thread, update_session_last_active, update_thread_last_active
)

"""
TODO -
1. Format assistant instruction as a JSON response and parse JSON to display in UI
2. Relatively high latency. Introduce a spinner/waiting/processing icon. Additionally, minimize latency
3. Ask AI to provide 3 top follow-up questions.
4. visual/gui workflow.
5. Enhance UI to improve UX.
6. 5 free messages, then sign up to use product.
7. UI reboot
"""

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


async def create_new_thread_and_run(session_token, prompt):
    logger.info("Creating new thread due to rate limit")
    new_thread = client.beta.threads.create(
        messages=[ {"role": "user", "content": prompt} ]
    )
    thread_id = new_thread.id
    encrypted_thread_token = create_thread(session_token, thread_id)

    logger.info(f"New thread created with id: {thread_id}")

    return thread_id, encrypted_thread_token


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
                # Retrieve messages, they are sorted with newest first
                messages = client.beta.threads.messages.list(thread_id=thread_id)
                logger.info(f"Retrieved {len(messages.data)} messages")

                # Find the most recent assistant message
                latest_assistant_message = next((msg for msg in messages.data if msg.role == "assistant"), None)

                if latest_assistant_message:
                    logger.info("Found latest assistant message, streaming content")
                    for content in latest_assistant_message.content:
                        if content.type == 'text':
                            # Stream each word separately
                            for word in content.text.value.split():
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
                    new_thread_id, new_thread_token = await create_new_thread_and_run(session_token, prompt)
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
                thread_id = get_latest_thread_for_session(session_token)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
