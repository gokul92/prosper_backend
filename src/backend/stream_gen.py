import os
import datetime
from openai import OpenAI
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from event_handling import EventHandler
from db_operations import get_thread_id_from_token  #, add_thread_id_and_token
from token_generator import generate_encrypted_token, match_encrypted_token

load_dotenv()

client = OpenAI(organization="org-vvQl4oYpM1sRl9uRk3PejmUS",
                project="proj_jAx3xZMcN9n2EVR2FhS8zsNk",
                api_key=os.getenv('OPENAI_SECRET_KEY'))

app = FastAPI()


def stream_generator(thread_id, assistant_id):
    with client.beta.threads.runs.stream(
            thread_id=thread_id,
            assistant_id=assistant_id,
            event_handler=EventHandler()) as stream:
        return stream.until_done()


class UserMessage(BaseModel):
    msg: str
    session_token: None | str = None
    expiry_time: None | int = None


@app.post('/chat/stream/')
async def stream_ai_response(response: UserMessage):
    response_dict = response.model_dump()
    if response_dict["session_token"] is None:
        sess_token = str(generate_encrypted_token())
        new_thread = client.beta.threads.create(
            messages=[
                {
                    "role": "user",
                    "content": response_dict['msg'],
                    "metadata": {
                        "token": sess_token,
                        "expiry_time": str(datetime.datetime.now().timestamp() + (24 * 60 * 60)),
                        "time_format": "unix_time"
                    }
                }
            ]
        )
        thread_id = new_thread.id
        """
        add thread and sess_token to database
        """
        # add_thread_id_and_token(thread_id, sess_token)

        return stream_generator(thread_id=thread_id,
                                assistant_id="asst_9od8x1PjV2GbjOcriSNcl2ph")
    elif match_encrypted_token(response_dict["session_token"]):
        thread_id = get_thread_id_from_token(bytes(response_dict["session_token"], 'utf-8'))
        _ = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=response.msg,
            metadata={
                "token": response.session_token,
                "expiry_time": "556678",  # replace with token expiry from db
                "time_format": "unix_time"
            }
        )
        return stream_generator(thread_id=thread_id,
                                assistant_id="asst_9od8x1PjV2GbjOcriSNcl2ph")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
