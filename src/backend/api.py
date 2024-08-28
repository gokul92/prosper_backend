from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from pydantic import BaseModel, UUID4, validator
from typing import Dict
import requests
import os
import psycopg
from dotenv import load_dotenv
from datetime import date
from uuid import uuid4
from psycopg import errors as psycopg_errors
import logging

logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

# Auth0 configuration
AUTH0_DOMAIN = os.get_env("AUTH0_DOMAIN")
API_AUDIENCE = os.get_env("API_AUDIENCE")
ALGORITHMS = ["RS256"]

# JWT token bearer
token_auth_scheme = HTTPBearer()

# Function to fetch Auth0 public key
def get_auth0_public_key():
    jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    jwks = requests.get(jwks_url).json()
    return jwks['keys'][0]

# Verify and decode JWT token
def verify_token(token: str) -> Dict:
    try:
        public_key = get_auth0_public_key()
        payload = jwt.decode(
            token,
            public_key,
            algorithms=ALGORITHMS,
            audience=API_AUDIENCE,
            issuer=f"https://{AUTH0_DOMAIN}/"
        )
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Dependency to get the current user from the token
def get_current_user(credentials: HTTPAuthorizationCredentials = Security(token_auth_scheme)):
    token = credentials.credentials
    payload = verify_token(token)
    return payload.get("sub")  # Return the user ID from the token

# Example protected endpoint
@app.post("/api/financial-advice")
async def get_financial_advice(user_data: Dict, current_user: str = Depends(get_current_user)):
    # Process the user data and generate financial advice
    # You can use the current_user ID to fetch user-specific data from your database if needed
    return {"advice": "Your personalized financial advice here"}

# Add more endpoints as needed, using the Depends(get_current_user) for authentication

# Error handler for authentication errors
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {"detail": exc.detail}, exc.status_code

# Pydantic model for account creation request
class AccountCreate(BaseModel):
    account_nickname: str
    brokerage: str
    account_type: str
    connection_type: str
    connection_status: str = "ACTIVE"

    @validator('connection_type')
    def validate_connection_type(cls, v):
        if v not in ['MANUAL', 'CSV', 'PDF', 'AUTOMATIC']:
            raise ValueError('Invalid connection type')
        return v

    @validator('connection_status')
    def validate_connection_status(cls, v):
        if v not in ['ACTIVE', 'INACTIVE']:
            raise ValueError('Invalid connection status')
        return v

# Database connection function
def get_db_connection():
    conn = psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    try:
        yield conn
    finally:
        conn.close()

@app.post("/create-account")
async def create_account(account: AccountCreate, current_user: str = Depends(get_current_user), conn: psycopg.Connection = Depends(get_db_connection)):
    account_id = str(uuid4())
    as_of_date = date.today()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO accounts (account_id, user_id, account_nickname, brokerage, account_type, connection_type, connection_status, as_of_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (account_id, current_user, account.account_nickname, account.brokerage, account.account_type, account.connection_type, account.connection_status, as_of_date))
        conn.commit()
    except psycopg_errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="Account with this ID already exists")
    except psycopg_errors.ForeignKeyViolation:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    except psycopg_errors.CheckViolation:
        raise HTTPException(status_code=400, detail="Invalid connection type or status")
    except psycopg_errors.NotNullViolation:
        raise HTTPException(status_code=400, detail="Missing required fields")
    except psycopg.Error as e:
        conn.rollback()
        logger.error(f"Database error while creating account: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    return {"status": "success", "message": "Account created successfully", "account_id": account_id, "as_of_date": as_of_date}

# New endpoint for adding account holdings
class AccountHolding(BaseModel):
    account_id: UUID4
    symbol: str
    quantity: float
    adjusted_cost_basis: float
    as_of_date: date = None

@app.post("/add-account-holdings")
async def add_account_holdings(holding: AccountHolding, current_user: str = Depends(get_current_user), conn: psycopg.Connection = Depends(get_db_connection)):
    try:
        with conn.cursor() as cur:
            # Check if the account exists and belongs to the current user
            cur.execute("SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s", (holding.account_id, current_user))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Account not found or doesn't belong to the current user")

            # Insert the holding
            cur.execute("""
                INSERT INTO account_holdings 
                (account_id, symbol, quantity, adjusted_cost_basis, as_of_date)
                VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_DATE))
            """, (
                holding.account_id,
                holding.symbol,
                holding.quantity,
                holding.adjusted_cost_basis,
                holding.as_of_date
            ))
        conn.commit()
    except psycopg_errors.ForeignKeyViolation:
        raise HTTPException(status_code=400, detail="Invalid account ID")
    except psycopg_errors.CheckViolation:
        raise HTTPException(status_code=400, detail="Quantity must be greater than 0")
    except psycopg_errors.NotNullViolation:
        raise HTTPException(status_code=400, detail="Missing required fields")
    except psycopg.Error as e:
        conn.rollback()
        logger.error(f"Database error while adding account holding: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

    return {"status": "success", "message": "Account holding added successfully"}
