import psycopg
from psycopg.rows import dict_row
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')


def get_connection():
    return psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR(255) PRIMARY KEY,
                    session_created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    session_active_status BOOLEAN NOT NULL DEFAULT TRUE,
                    session_inactivated_timestamp TIMESTAMP,
                    last_active_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id VARCHAR(255) PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    thread_created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    thread_active_status BOOLEAN NOT NULL DEFAULT TRUE,
                    last_active_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            ''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_threads_session_id ON threads(session_id)')
        conn.commit()


def create_session():
    session_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sessions (session_id) VALUES (%s) RETURNING *",
                (session_id,)
            )
            new_session = cur.fetchone()
        conn.commit()
    return new_session[ 'session_id' ]


def update_session_last_active(session_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET last_active_timestamp = CURRENT_TIMESTAMP WHERE session_id = %s RETURNING *",
                (session_id,)
            )
            updated_session = cur.fetchone()
        conn.commit()
    return updated_session


def create_thread(session_id):
    thread_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO threads (thread_id, session_id) VALUES (%s, %s) RETURNING *",
                (thread_id, session_id)
            )
            new_thread = cur.fetchone()
        conn.commit()
    return new_thread[ 'thread_id' ]


def update_thread_last_active(thread_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE threads SET last_active_timestamp = CURRENT_TIMESTAMP WHERE thread_id = %s RETURNING *",
                (thread_id,)
            )
            updated_thread = cur.fetchone()
        conn.commit()
    return updated_thread


def get_session_id_from_thread(thread_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT session_id FROM threads WHERE thread_id = %s",
                (thread_id,)
            )
            result = cur.fetchone()
    return result[ 'session_id' ] if result else None


def list_threads_for_session(session_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM threads WHERE session_id = %s ORDER BY last_active_timestamp DESC",
                (session_id,)
            )
            threads = cur.fetchall()
    return threads


def get_latest_thread_for_session(session_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM threads WHERE session_id = %s ORDER BY last_active_timestamp DESC LIMIT 1",
                (session_id,)
            )
            latest_thread = cur.fetchone()
    return latest_thread


def terminate_thread(thread_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE threads 
                SET thread_active_status = FALSE,
                    thread_inactivated_timestamp = CURRENT_TIMESTAMP 
                WHERE thread_id = %s
                RETURNING *
                """,
                (thread_id,)
            )
            terminated_thread = cur.fetchone()
        conn.commit()
    return terminated_thread


def terminate_session(session_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Update all threads for the session
            cur.execute(
                """
                UPDATE threads 
                SET thread_active_status = FALSE,
                    thread_inactivated_timestamp = CURRENT_TIMESTAMP 
                WHERE session_id = %s
                """,
                (session_id,)
            )
            # Update the session
            cur.execute(
                """
                UPDATE sessions 
                SET session_active_status = FALSE, 
                    session_inactivated_timestamp = CURRENT_TIMESTAMP 
                WHERE session_id = %s 
                RETURNING *
                """,
                (session_id,)
            )
            terminated_session = cur.fetchone()
        conn.commit()
    return terminated_session


# Call this function when starting your application
init_db()
