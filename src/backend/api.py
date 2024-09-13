from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from pydantic import BaseModel, UUID4, validator, Field, EmailStr
from typing import Dict, List, Optional
import requests
import os
import psycopg
from datetime import date
from dotenv import load_dotenv
from datetime import date, datetime
from uuid import uuid4
from psycopg import errors as psycopg_errors
import logging
from urllib.parse import urlparse
from fastapi import Body
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from ..utils.validators import validate_payload
from ..domain_logic.simulation import simulate_balance_paths
from ..utils.calcs import portfolio_statistics
from ..domain_logic.tax_rates_calc import IncomeTaxRates
import json
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime
from functools import lru_cache
import hashlib
from contextlib import asynccontextmanager
from fastapi.exceptions import RequestValidationError

logger = logging.getLogger(__name__)

load_dotenv()

# Define the lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("Starting up the application")
    try:
        # Create a new connection for this startup task
        conn = psycopg.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS monte_carlo_cache (
                    cache_key TEXT PRIMARY KEY,
                    result JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()
        logger.info("monte_carlo_cache table created successfully")
    except psycopg.Error as e:
        logger.error(f"Error creating monte_carlo_cache table: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
    
    yield  # This is where the app runs
    
    # Shutdown logic
    logger.info("Shutting down the application")
    # Perform any cleanup tasks here if needed

# Create the FastAPI app with the lifespan
app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Add your frontend URL
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Auth0 configuration
AUTH0_DOMAIN = urlparse(os.getenv("AUTH0_DOMAIN")).netloc
API_AUDIENCE = os.getenv("API_AUDIENCE")
ALGORITHMS = ["RS256"]

# JWT token bearer
token_auth_scheme = HTTPBearer()


# Function to fetch Auth0 public key
def get_auth0_public_key():
    jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    jwks = requests.get(jwks_url).json()
    return jwks["keys"][0]


# Verify and decode JWT token
def verify_token(token: str) -> Dict:
    try:
        public_key = get_auth0_public_key()
        payload = jwt.decode(
            token,
            public_key,
            algorithms=ALGORITHMS,
            audience=API_AUDIENCE,
            issuer=f"https://{AUTH0_DOMAIN}/",
        )
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# Dependency to get the current user from the token
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(token_auth_scheme),
):
    token = credentials.credentials
    payload = verify_token(token)
    return payload.get("sub")  # Return the user ID from the token


# Error handler for authentication errors
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


# Pydantic model for account creation request
class AccountCreate(BaseModel):
    account_nickname: str
    brokerage: str
    account_type: str
    connection_type: str
    connection_status: str = "ACTIVE"

    @validator("connection_type")
    def validate_connection_type(cls, v):
        if v not in ["MANUAL", "CSV", "PDF", "AUTOMATIC"]:
            raise ValueError("Invalid connection type")
        return v

    @validator("connection_status")
    def validate_connection_status(cls, v):
        if v not in ["ACTIVE", "INACTIVE"]:
            raise ValueError("Invalid connection status")
        return v


# Database connection function
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    conn = psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        row_factory=dict_row
    )
    try:
        yield conn
    finally:
        conn.close()


@app.post("/create-account")
def create_account(
    account: AccountCreate,
    current_user: str = Depends(get_current_user)
):
    with get_db_connection() as conn:
        account_id = str(uuid4())
        as_of_date = date.today()

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO accounts (account_id, user_id, account_nickname, brokerage, account_type, connection_type, connection_status, as_of_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        account_id,
                        current_user,
                        account.account_nickname,
                        account.brokerage,
                        account.account_type,
                        account.connection_type,
                        account.connection_status,
                        as_of_date,
                    ),
                )
            conn.commit()
        except psycopg_errors.UniqueViolation:
            raise HTTPException(
                status_code=409, detail="Account with this ID already exists"
            )
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

        return {
            "status": "success",
            "message": "Account created successfully",
            "account_id": account_id,
            "as_of_date": as_of_date,
        }


# New endpoint for adding account holdings
class AccountHolding(BaseModel):
    symbol: str
    quantity: float
    adjusted_cost_basis: float
    as_of_date: date = None

    @validator("quantity")
    def quantity_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Quantity must be greater than 0")
        return v


class AccountHoldingsCreate(BaseModel):
    account_id: UUID4
    holdings: List[AccountHolding]


@app.post("/add-account-holdings")
def add_account_holdings(
    holdings_data: AccountHoldingsCreate,
    current_user: str = Depends(get_current_user)
):
    with get_db_connection() as conn:
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                # Check if the account exists and belongs to the current user
                cur.execute(
                    "SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s",
                    (holdings_data.account_id, current_user),
                )
                if cur.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Account not found or doesn't belong to the current user",
                    )

                # Validate symbols against security_master
                symbols = [holding.symbol for holding in holdings_data.holdings]
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        security_type,
                        security_name
                    FROM security_master
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, CASE 
                        WHEN security_type != 'Equity' THEN 0
                        ELSE 1
                    END, as_of_date DESC
                """,
                    (symbols,),
                )
                validated_symbols = cur.fetchall()

                unknown_symbols = [
                    symbol
                    for symbol in symbols
                    if symbol not in [vs["symbol"] for vs in validated_symbols]
                ]

                if unknown_symbols:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unknown symbols: {', '.join(unknown_symbols)}",
                    )

                # Prepare the query for inserting multiple holdings
                insert_query = """
                    INSERT INTO account_holdings 
                    (account_id, symbol, quantity, adjusted_cost_basis, as_of_date)
                    VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_DATE))
                """

                # Prepare the data for bulk insert
                insert_data = [
                    (
                        holdings_data.account_id,
                        holding.symbol,
                        holding.quantity,
                        holding.adjusted_cost_basis,
                        holding.as_of_date,
                    )
                    for holding in holdings_data.holdings
                ]

                # Execute the bulk insert
                cur.executemany(insert_query, insert_data)

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except psycopg_errors.ForeignKeyViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Invalid account ID")
        except psycopg_errors.CheckViolation as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail=f"Constraint violation: {str(e)}")
        except psycopg_errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(
                status_code=409, detail="Duplicate entry for account holdings"
            )
        except psycopg_errors.NotNullViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Missing required fields")
        except psycopg.Error as e:
            conn.rollback()
            logger.error(f"Database error while adding account holdings: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

        return {
            "status": "success",
            "message": f"Added {len(holdings_data.holdings)} holdings to the account",
        }


# New endpoint for receiving tax rates information
class TaxRatesInformation(BaseModel):
    user_id: str
    state: str
    annual_income: float = Field(..., ge=0)
    age: int = Field(..., ge=1, le=150)
    filing_status: str


@app.post("/tax-rates")
def calculate_tax_rates(
    tax_info: dict,
    current_user: str = Depends(get_current_user)
):
    with get_db_connection() as conn:
        validated_tax_info = validate_payload(TaxRatesInformation, tax_info)

        # Verify that the current_user matches the user_id in the payload
        if current_user != validated_tax_info.user_id:
            raise HTTPException(status_code=403, detail="User ID mismatch")

        try:
            # Insert tax rates information
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO tax_rates_information 
                    (user_id, state, annual_income, age, filing_status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING tax_rates_information_id, as_of_date
                """,
                    (
                        validated_tax_info.user_id,
                        validated_tax_info.state,
                        validated_tax_info.annual_income,
                        validated_tax_info.age,
                        validated_tax_info.filing_status,
                    ),
                )
                result = cur.fetchone()
                tax_rates_information_id = result["tax_rates_information_id"]
                as_of_date = result["as_of_date"]

            # Calculate tax rates
            tax_calculator = IncomeTaxRates(
                validated_tax_info.state,
                validated_tax_info.annual_income,
                validated_tax_info.filing_status,
            )
            income_tax_rates = tax_calculator.calculate_income_tax_rate()
            ltcg_rates = tax_calculator.calculate_ltcg_rate()

            # Insert calculated tax rates
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tax_rates 
                    (tax_rates_information_id, user_id, federal_income_tax_rate, state_income_tax_rate, 
                    federal_long_term_capital_gains_rate, state_long_term_capital_gains_rate, as_of_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        tax_rates_information_id,
                        validated_tax_info.user_id,
                        income_tax_rates["federal_rate"],
                        income_tax_rates["state_rate"],
                        ltcg_rates["federal_rate"],
                        ltcg_rates["state_rate"],
                        as_of_date,
                    ),
                )

            conn.commit()

            # Prepare response
            response = {
                "status": "success",
                "federal_income_tax_rate": income_tax_rates["federal_rate"],
                "state_income_tax_rate": income_tax_rates["state_rate"],
                "federal_long_term_capital_gains_rate": ltcg_rates["federal_rate"],
                "state_long_term_capital_gains_rate": ltcg_rates["state_rate"],
            }

            return response

        except psycopg.Error as e:
            conn.rollback()
            logger.error(f"Database error while processing tax rates: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")


# Pydantic model for user creation request
class UserCreate(BaseModel):
    user_id: str
    email_id: EmailStr
    name: str = None
    nickname: str = None
    picture: str = None
    email_verified: bool = False
    user_created_at: datetime = None

@app.post("/add-user")
def add_user(user: UserCreate):
    with get_db_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (user_id, email_id, name, nickname, picture, email_verified, user_created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user.user_id,
                        user.email_id,
                        user.name,
                        user.nickname,
                        user.picture,
                        user.email_verified,
                        user.user_created_at or datetime.utcnow(),
                    ),
                )
            conn.commit()
        except psycopg_errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(
                status_code=409, detail="User with this ID or email already exists"
            )
        except psycopg_errors.NotNullViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Missing required fields")
        except psycopg.Error as e:
            conn.rollback()
            logger.error(f"Database error while adding user: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

        return {
            "status": "success",
            "message": "User added successfully",
            "user_id": user.user_id
        }

def fetch_account_statistics(
    account_id: UUID4,
    current_user: str,
    conn: psycopg.Connection
) -> dict:
    try:
        # Check if the account belongs to the current user
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s",
                (account_id, current_user)
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail="Account not found or doesn't belong to the current user"
                )

        as_of_date = date.today().isoformat()
        # Calculate portfolio statistics
        stats = portfolio_statistics(str(account_id), as_of_date)

        return {
            "account_id": str(account_id),
            "account_balance": stats["account_balance"],
            "annual_mean_return": stats["return"],
            "annual_std_dev": stats["volatility"]
        }

    except psycopg.Error as e:
        logger.error(f"Database error while fetching account statistics: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        logger.error(f"Error while calculating account statistics: {str(e)}")
        raise HTTPException(status_code=500, detail="Error calculating account statistics")

@app.get("/account-statistics/{account_id}")
def get_account_statistics(
    account_id: UUID4,
    current_user: str = Depends(get_current_user)
):
    with get_db_connection() as conn:
        return fetch_account_statistics(account_id, current_user, conn)


# LRU cache for in-memory caching
@lru_cache(maxsize=100)
def get_cached_simulation(cache_key: str):
    return None  # This will be updated if we find a cached result

# Function to generate a cache key
def generate_cache_key(account_id: str, current_user: str):
    key_data = f"{account_id}:{current_user}"
    return hashlib.md5(key_data.encode()).hexdigest()

from fastapi import HTTPException, Body
from pydantic import BaseModel, UUID4

class MonteCarloSimulationRequest(BaseModel):
    account_id: UUID4

@app.post("/monte-carlo-simulation")
def monte_carlo_simulation(
    request: MonteCarloSimulationRequest,
    current_user: str = Depends(get_current_user)
):
    try:
        account_id = str(request.account_id)
        
        # Generate cache key
        cache_key = generate_cache_key(account_id, current_user)

        # Check in-memory cache
        cached_result = get_cached_simulation(cache_key)
        if cached_result:
            return cached_result

        with get_db_connection() as conn:
            # Check database cache
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT result FROM monte_carlo_cache WHERE cache_key = %s",
                    (cache_key,)
                )
                db_cached_result = cur.fetchone()
                if db_cached_result:
                    # Update in-memory cache and return result
                    get_cached_simulation.cache_clear()
                    get_cached_simulation(cache_key)
                    return db_cached_result['result']

            # If not cached, perform the simulation
            # Fetch account details
            account_stats = fetch_account_statistics(account_id, current_user, conn)

            # Simulate balance paths
            start_date = date.today().isoformat()
            simulation_result = simulate_balance_paths(
                starting_balance=account_stats["account_balance"],
                annual_mean=account_stats["annual_mean_return"],
                annual_std_dev=account_stats["annual_std_dev"],
                start_date=start_date
            )

            # Prepare response
            response = {
                "account_id": account_id,
                "account_info": account_stats,
                "simulation_start_date": start_date,
                "dates": simulation_result["dates"],
                "balance_paths": simulation_result["balance_paths"].to_dict(),
                "percentile_95_balance_path": simulation_result["percentile_95_balance_path"].to_dict(),
                "percentile_5_balance_path": simulation_result["percentile_5_balance_path"].to_dict(),
                "prob_95_percentile": float(simulation_result["prob_95_percentile"]),
                "prob_5_percentile": float(simulation_result["prob_5_percentile"]),
                "final_balance_min": float(simulation_result["final_balance_min"]),
                "final_balance_max": float(simulation_result["final_balance_max"]),
                "final_95_percentile_balance": float(simulation_result["final_95_percentile_balance"]),
                "final_5_percentile_balance": float(simulation_result["final_5_percentile_balance"]),
                "prob_greater_than_starting": float(simulation_result["prob_greater_than_starting"]),
                "prob_between_starting_and_95": float(simulation_result["prob_between_starting_and_95"]),
                "prob_between_5_and_starting": float(simulation_result["prob_between_5_and_starting"]),
                "percentile_95_balance_1y": float(simulation_result["percentile_95_balance_1y"]),
                "percentile_5_balance_1y": float(simulation_result["percentile_5_balance_1y"])
            }

            # Write response to JSON file
            # data_dir = Path(__file__).parent.parent.parent / 'data' / 'temp_debug'
            # data_dir.mkdir(exist_ok=True)
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # file_name = f"monte_carlo_simulation_{account_id}_{timestamp}.json"
            # file_path = data_dir / file_name

            # with open(file_path, 'w') as f:
            #     json.dump(response, f, indent=2, default=str)

            # logger.info(f"Monte Carlo simulation result saved to {file_path}")

            # Cache the result
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO monte_carlo_cache (cache_key, result) VALUES (%s, %s) ON CONFLICT (cache_key) DO UPDATE SET result = EXCLUDED.result, created_at = CURRENT_TIMESTAMP",
                    (cache_key, json.dumps(response))
                )
            conn.commit()

            # Update in-memory cache
            get_cached_simulation.cache_clear()
            get_cached_simulation(cache_key)

            return response

    except RequestValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))
    except psycopg.Error as e:
        logger.error(f"Database error during Monte Carlo simulation: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        logger.error(f"Error during Monte Carlo simulation: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Error details: {e.args}")
        raise HTTPException(status_code=500, detail="Error during simulation")

# Add a new endpoint to clear the cache if needed
@app.post("/clear-monte-carlo-cache")
def clear_monte_carlo_cache(
    current_user: str = Depends(get_current_user)
):
    with get_db_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM monte_carlo_cache")
            conn.commit()
            get_cached_simulation.cache_clear()
            return {"message": "Monte Carlo simulation cache cleared"}
        except psycopg.Error as e:
            logger.error(f"Database error while clearing Monte Carlo cache: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
