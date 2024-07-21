import asyncio
import datetime
import json
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from openai import OpenAI
from pydantic import BaseModel

from db_operations import get_thread_id_from_token  # , add_thread_id_and_token
from token_generator import generate_encrypted_token, match_encrypted_token

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


async def stream_generator(thread_id, assistant_id):
    try:
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        while True:
            run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            if run.status == 'completed':
                messages = client.beta.threads.messages.list(thread_id=thread_id)
                for msg in messages:
                    if msg.role == "assistant":
                        for content in msg.content:
                            if content.type == 'text':
                                # Stream each word separately
                                for word in content.text.value.split():
                                    yield word + ' '
                                    await asyncio.sleep(0.1)  # Small delay between words
                break
            elif run.status in [ 'failed', 'cancelled', 'expired' ]:
                logger.error(f"Run ended with status: {run.status}")
                break
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in stream_generator: {str(e)}")
        yield None


class UserMessage(BaseModel):
    prompt: str
    session_token: None | str = None
    expiry_time: None | int = None


@app.options("/chat/stream")
async def options_chat_stream():
    return {"message": "OK"}


@app.post('/chat/stream')
async def stream_ai_response(response: UserMessage):
    logger.info(f"Received request: {response}")
    response_dict = response.model_dump()

    async def event_generator():
        try:
            if (response_dict[ "session_token" ] is None) or (
                    match_encrypted_token(response_dict[ 'session_token' ] is False)):
                sess_token = str(generate_encrypted_token())
                new_thread = client.beta.threads.create(
                    messages=[
                        {
                            "role": "user",
                            "content": response_dict[ 'prompt' ],
                            "metadata": {
                                "token": sess_token,
                                "expiry_time": str(datetime.datetime.now().timestamp() + (24 * 60 * 60)),
                                "time_format": "unix_time"
                            }
                        }
                    ]
                )
                thread_id = new_thread.id
                # TODO: Uncomment when ready to use database
                # background_tasks.add_task(add_thread_id_and_token, thread_id, sess_token)
            else:
                thread_id = get_thread_id_from_token(bytes(response_dict[ "session_token" ], 'utf-8'))

                client.beta.threads.messages.create(
                    thread_id=thread_id,
                    role="user",
                    content=response_dict[ "prompt" ],
                    metadata={
                        "token": response_dict[ "session_token" ],
                        "expiry_time": "556678",  # replace with token expiry from db
                        "time_format": "unix_time"
                    }
                )

            async for chunk in stream_generator(thread_id=thread_id, assistant_id="asst_9od8x1PjV2GbjOcriSNcl2ph"):
                if chunk is None:
                    break
                yield f"data: {json.dumps({'type': 'message', 'content': chunk})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'content': 'Stream finished'})}\n\n"
        except Exception as e:
            logger.error(f"Error in event_generator: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
