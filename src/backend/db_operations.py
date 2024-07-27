import psycopg
from psycopg.rows import dict_row
import uuid
import os
from dotenv import load_dotenv
from token_generator import encrypt_id

load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')

"""
TODO 
retrieve all messages associated with a thread from openai
retrieve all messages assoociated with a user from openai
retrieve latest message associated with a thread from openai
"""


def get_connection():
    return psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR(255) PRIMARY KEY,
                    encrypted_token TEXT UNIQUE NOT NULL,
                    session_created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    session_active_status BOOLEAN NOT NULL DEFAULT TRUE,
                    session_inactivated_timestamp TIMESTAMP DEFAULT NULL,
                    last_active_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id VARCHAR(255) PRIMARY KEY,
                    encrypted_thread TEXT UNIQUE NOT NULL,
                    session_id VARCHAR(255) NOT NULL,
                    thread_created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    thread_active_status BOOLEAN NOT NULL DEFAULT TRUE,
                    last_active_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    thread_inactivated_timestamp TIMESTAMP DEFAULT NULL,
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            ''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_threads_session_id ON threads(session_id)')
        conn.commit()


def create_session():
    session_id = str(uuid.uuid4())
    encrypted_token = encrypt_id(session_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sessions (session_id, encrypted_token) VALUES (%s, %s) RETURNING encrypted_token",
                (session_id, encrypted_token)
            )
            new_session = cur.fetchone()
        conn.commit()
    return new_session[ 'encrypted_token' ]


def validate_session_token(encrypted_token):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM sessions WHERE encrypted_token = %s AND session_active_status = TRUE",
                (encrypted_token,)
            )
            new_session = cur.fetchone()
        conn.commit()
    if new_session and new_session[ 'encrypted_token' ]:
        return True
    else:
        return False


def update_session_last_active(encrypted_token):
    session = validate_session_token(encrypted_token)
    if session:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE sessions SET last_active_timestamp = CURRENT_TIMESTAMP WHERE encrypted_token = %s "
                    "RETURNING encrypted_token",
                    (encrypted_token,)
                )
                updated_session = cur.fetchone()
            conn.commit()
        return updated_session[ 'encrypted_token' ]
    return None


def create_thread(encrypted_token, thread_id=None):
    session = validate_session_token(encrypted_token)
    if session:
        if not thread_id:
            thread_id = str(uuid.uuid4())
        encrypted_thread = encrypt_id(thread_id)
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get the session_id from the sessions table
                cur.execute(
                    "SELECT session_id FROM sessions WHERE encrypted_token = %s",
                    (encrypted_token,)
                )
                session_id = cur.fetchone()
                cur.execute(
                    "INSERT INTO threads (thread_id, encrypted_thread, session_id) VALUES (%s, %s, %s) "
                    "RETURNING encrypted_thread",
                    (thread_id, encrypted_thread, session_id[ 'session_id' ])
                )
                new_thread = cur.fetchone()
                conn.commit()
                return new_thread[ 'encrypted_thread' ]
    return None


def validate_thread_token(encrypted_thread):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.*, s.session_active_status 
                FROM threads t 
                JOIN sessions s ON t.session_id = s.session_id 
                WHERE t.encrypted_thread = %s AND t.thread_active_status = TRUE
                """,
                (encrypted_thread,)
            )
            thread = cur.fetchone()
    if thread and thread[ 'encrypted_thread' ]:
        return True
    else:
        return False


def update_thread_last_active(encrypted_thread):
    thread = validate_thread_token(encrypted_thread)
    if thread:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE threads SET last_active_timestamp = CURRENT_TIMESTAMP WHERE encrypted_thread = %s "
                    "RETURNING encrypted_thread",
                    (encrypted_thread,)
                )
                updated_thread = cur.fetchone()
            conn.commit()
        return updated_thread[ 'encrypted_thread' ]
    return None


def get_encrypted_token_from_thread(encrypted_thread):
    thread = validate_thread_token(encrypted_thread)
    if thread:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_id FROM threads where encrypted_thread = %s",
                    (encrypted_thread,)
                )
                session_id = cur.fetchone()
                cur.execute(
                    "SELECT encrypted_token FROM sessions WHERE session_id = %s",
                    (session_id[ 'session_id' ],)
                )
                result = cur.fetchone()
        return result[ 'encrypted_token' ] if result else None
    return None


def validate_session_and_thread_tokens(encrypted_session_token, encrypted_thread_token=None):
    session_valid = validate_session_token(encrypted_session_token)

    if not session_valid:
        return {'session': False,
                'thread': False}

    if encrypted_thread_token == '' and session_valid:
        return {'session': True,
                'thread': False}

    thread_valid = validate_thread_token(encrypted_thread_token)

    if thread_valid and session_valid:
        # Check if the thread belongs to the session
        thread_session_token = get_encrypted_token_from_thread(encrypted_thread_token)
        if thread_session_token == encrypted_session_token:
            return {'session': True,
                    'thread': True}
        else:
            return {'session': True,
                    'thread': False}


def list_threads_for_session(encrypted_token):
    session = validate_session_token(encrypted_token)
    if session:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_id FROM sessions where encrypted_token = %s",
                    (encrypted_token,)
                )
                session_id = cur.fetchone()
                cur.execute(
                    "SELECT encrypted_thread, last_active_timestamp FROM threads WHERE session_id = %s "
                    "ORDER BY last_active_timestamp DESC",
                    (session_id[ 'session_id' ],)
                )
                threads = cur.fetchall()
        return threads
    return None


def get_latest_thread_for_session(encrypted_token):
    session = validate_session_token(encrypted_token)
    if session:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_id FROM sessions where encrypted_token = %s",
                    (encrypted_token,)
                )
                session_id = cur.fetchone()
                cur.execute(
                    "SELECT thread_id FROM threads WHERE session_id = %s AND thread_active_status = TRUE "
                    "ORDER BY last_active_timestamp DESC LIMIT 1",
                    (session_id[ 'session_id' ],)
                )
                latest_thread = cur.fetchone()
        return latest_thread[ 'thread_id' ] if latest_thread else None
    return None


def terminate_thread(encrypted_thread):
    thread = validate_thread_token(encrypted_thread)
    if thread:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE threads 
                    SET thread_active_status = FALSE,
                        thread_inactivated_timestamp = CURRENT_TIMESTAMP 
                    WHERE thread_id = %s
                    RETURNING encrypted_thread
                    """,
                    (encrypted_thread,)
                )
                terminated_thread = cur.fetchone()
            conn.commit()
        return terminated_thread[ 'encrypted_thread' ]
    return None


def terminate_session(encrypted_token):
    session = validate_session_token(encrypted_token)
    if session:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_id FROM sessions where encrypted_token = %s",
                    (encrypted_token,)
                )
                session_id = cur.fetchone()
                # Update all threads for the session
                cur.execute(
                    """
                    UPDATE threads 
                    SET thread_active_status = FALSE,
                        thread_inactivated_timestamp = CURRENT_TIMESTAMP 
                    WHERE session_id = %s
                    """,
                    (session_id[ 'session_id' ],)
                )
                # Update the session
                cur.execute(
                    """
                    UPDATE sessions 
                    SET session_active_status = FALSE, 
                        session_inactivated_timestamp = CURRENT_TIMESTAMP 
                    WHERE session_id = %s 
                    RETURNING encrypted_token
                    """,
                    (session_id[ 'session_id' ],)
                )
                terminated_session = cur.fetchone()
            conn.commit()
        return terminated_session[ 'encrypted_token' ]
    return None


# Call this function when starting your application
init_db()
