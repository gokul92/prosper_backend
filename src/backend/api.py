from fastapi import FastAPI, Depends, HTTPException, Security, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from pydantic import BaseModel, UUID4, validator, Field, EmailStr
from typing import Any, Dict, List, Optional
import requests
import os
import psycopg
from dotenv import load_dotenv
from datetime import date, datetime, timezone
from uuid import uuid4, UUID
from psycopg import errors as psycopg_errors
import logging
from urllib.parse import urlparse
from fastapi.encoders import jsonable_encoder
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from ..utils.validators import validate_payload
from ..utils.calcs import portfolio_statistics, optimized_portfolio_stats, tax_optimized_rebalance, fetch_account_data, fetch_portfolio_data, calculate_asset_allocation
from ..domain_logic.tax_rates_calc import IncomeTaxRates
import json
from contextlib import asynccontextmanager
from datetime import date, datetime
from functools import lru_cache
import hashlib
from contextlib import asynccontextmanager
from fastapi.exceptions import RequestValidationError
from src.utils.json_utils import CustomJSONEncoder, process_for_json
import math
from collections import defaultdict
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

#TODO - add account number to the account create
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
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

        return {
            "status": "success",
            "message": "Account created successfully",
            "account_id": str(account_id),  # Convert UUID to string for JSON response
            "as_of_date": as_of_date,
        }


class AccountHolding(BaseModel):
    symbol: str
    quantity: float
    adjusted_cost_basis: float
    as_of_date: date = None
    purchase_date: date = None
    capital_gains_type: str = None
    total_cost: float = None
    security_type: str = None
    cusip: str = None
    mstar_id: str = None

    @validator("quantity")
    def quantity_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Quantity must be greater than 0")
        return v

    @validator("capital_gains_type")
    def validate_capital_gains_type(cls, v):
        if v is not None and v not in ["lt", "st"]:
            raise ValueError("Capital gains type must be either 'lt' or 'st'")
        return v

    @validator("purchase_date")
    def validate_purchase_date(cls, v, values):
        if v is not None and "as_of_date" in values and values["as_of_date"] is not None:
            if v > values["as_of_date"]:
                raise ValueError("Purchase date cannot be after as_of_date")
        return v

    @validator("total_cost")
    def validate_total_cost(cls, v, values):
        if v is not None:
            if v <= 0:
                raise ValueError("Total cost must be greater than 0")
            if "quantity" in values and "adjusted_cost_basis" in values:
                expected_total_cost = values["quantity"] * values["adjusted_cost_basis"]
                if not math.isclose(v, expected_total_cost, rel_tol=0.05):
                    raise ValueError("Total cost should be to within 5% of quantity * adjusted_cost_basis")
        return v

class AccountHoldingsCreate(BaseModel):
    account_id: UUID4
    holdings: List[AccountHolding]


logger = logging.getLogger(__name__)

@app.post("/add-account-holdings")
def add_account_holdings(
    holdings_data: AccountHoldingsCreate,
    current_user: str = Depends(get_current_user)
):
    logger.info(f"Received request to add holdings for account: {holdings_data.account_id}")
    logger.debug(f"Holdings data: {holdings_data.dict()}")

    with get_db_connection() as conn:
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                # Check if the account exists and belongs to the current user
                logger.debug(f"Checking if account exists for user: {current_user}")
                cur.execute(
                    "SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s",
                    (holdings_data.account_id, current_user)
                )
                if cur.fetchone() is None:
                    logger.warning(f"Account not found or doesn't belong to user. Account ID: {holdings_data.account_id}, User ID: {current_user}")
                    raise HTTPException(
                        status_code=404,
                        detail=f"Account not found or doesn't belong to the current user. Account ID: {holdings_data.account_id}, User ID: {current_user}",
                    )

                logger.debug("Account found, proceeding with holdings validation")

                # Validate symbols against security_master and adjust as_of_date
                for holding in holdings_data.holdings:
                    logger.debug(f"Validating holding: {holding.dict()}")

                    # Set default as_of_date if not provided
                    holding_as_of_date = holding.as_of_date or date.today()

                    query = """
                    SELECT 
                        symbol, 
                        security_type, 
                        security_name, 
                        cusip, 
                        mstar_id,
                        as_of_date
                    FROM security_master
                    WHERE symbol = %s
                    """
                    params = [holding.symbol]

                    if holding.security_type:
                        query += " AND security_type = %s"
                        params.append(holding.security_type)
                    if holding.cusip:
                        query += " AND cusip = %s"
                        params.append(holding.cusip)

                    query += " AND as_of_date <= %s"
                    params.append(holding_as_of_date)

                    query += " ORDER BY as_of_date DESC LIMIT 1"

                    logger.debug(f"Executing query: {query} with params: {params}")
                    cur.execute(query, params)
                    security = cur.fetchone()

                    if not security:
                        logger.warning(f"Invalid symbol, security_type, or cusip combination or no matching as_of_date for symbol: {holding.symbol}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid symbol, security_type, or cusip combination or no matching as_of_date for symbol: {holding.symbol}"
                        )

                    logger.debug(f"Security found: {security}")
                    # Update holding with fetched data
                    holding.security_type = security['security_type']
                    holding.cusip = security['cusip']
                    if 'mstar_id' in security:
                        holding.mstar_id = security['mstar_id']
                    # Use the as_of_date from security_master to satisfy foreign key constraint
                    holding.as_of_date = security['as_of_date']

                logger.debug("All holdings validated, preparing for insertion")

                # Prepare the query for inserting multiple holdings
                insert_query = """
                    INSERT INTO account_holdings 
                    (account_id, symbol, quantity, adjusted_cost_basis, as_of_date, 
                     purchase_date, capital_gains_type, total_cost, security_type, cusip, mstar_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Prepare the data for bulk insert
                insert_data = [
                    (
                        holdings_data.account_id,
                        holding.symbol,
                        holding.quantity,
                        holding.adjusted_cost_basis,
                        holding.as_of_date,
                        holding.purchase_date,
                        holding.capital_gains_type,
                        holding.total_cost,
                        holding.security_type,
                        holding.cusip,
                        holding.mstar_id
                    )
                    for holding in holdings_data.holdings
                ]

                logger.debug(f"Executing bulk insert with {len(insert_data)} holdings")
                logger.debug(f"Insert query: {insert_query}")
                logger.debug(f"Insert data: {insert_data}")
                
                try:
                    # Execute the bulk insert
                    cur.executemany(insert_query, insert_data)
                except psycopg.errors.ForeignKeyViolation as e:
                    logger.error(f"Foreign key violation during bulk insert: {str(e)}")
                    raise HTTPException(status_code=400, detail=f"Foreign key violation: {str(e)}")
                except psycopg.Error as e:
                    logger.error(f"Database error during bulk insert: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Database error during bulk insert: {str(e)}")

            conn.commit()
            logger.info(f"Successfully added {len(holdings_data.holdings)} holdings to account {holdings_data.account_id}")
        except HTTPException as http_exc:
            logger.exception("HTTP exception occurred")
            raise http_exc
        except psycopg_errors.ForeignKeyViolation:
            raise HTTPException(status_code=400, detail="Invalid account ID")
        except psycopg_errors.CheckViolation as e:
            raise HTTPException(status_code=400, detail=f"Constraint violation: {str(e)}")
        except psycopg_errors.UniqueViolation:
            raise HTTPException(
                status_code=409, detail="Duplicate entry for account holdings"
            )
        except psycopg_errors.NotNullViolation as e:
            raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")
        except psycopg.Error as e:
            conn.rollback()
            logger.exception(f"Database error while adding account holdings: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

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

def validate_account_id(account_id: str, current_user: str, conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s",
            (account_id, current_user)
        )
        return cur.fetchone() is not None

# TODO - Create a portfolios table and use it here. For now, use the portfolio_holdings table
def validate_portfolio_id(portfolio_id: str, user_id: str, conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM portfolio_holdings WHERE portfolio_id = %s AND user_id = %s LIMIT 1
        """, (portfolio_id, user_id))
        return cur.fetchone() is not None

def fetch_account_statistics(account_id: str, current_user: str, conn: psycopg.Connection) -> dict:
    if not validate_account_id(account_id, current_user, conn):
        raise HTTPException(
            status_code=404,
            detail="Account not found or doesn't belong to the current user"
        )
    
    # Call portfolio_statistics from calcs.py
    return portfolio_statistics(account_id, date.today().isoformat())

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
def generate_cache_key(account_id: str, current_user: str, as_of_date: str):
    key_data = f"{account_id}:{current_user}:{as_of_date}"
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
        as_of_date = date.today().isoformat()  # Use today's date as as_of_date
        
        with get_db_connection() as conn:
            # Validate account ID
            if not validate_account_id(account_id, current_user, conn):
                raise HTTPException(
                    status_code=404,
                    detail="Account not found or doesn't belong to the current user"
                )

            # Generate cache key with as_of_date
            cache_key = generate_cache_key(account_id, current_user, as_of_date)

            # Check in-memory cache
            cached_result = get_cached_simulation(cache_key)
            if cached_result:
                return JSONResponse(content=jsonable_encoder(cached_result))

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
                    return JSONResponse(content=jsonable_encoder(db_cached_result['result']))

            # If not cached, perform the simulation
            simulation_result = optimized_portfolio_stats(account_id, as_of_date)

            # Prepare response
            result = {
                "account_id": account_id,
                "simulation_start_date": as_of_date,
                "original_stats": simulation_result['original_stats'],
                "optimized_portfolios": simulation_result['optimized_portfolios']
            }

            processed_result = process_for_json(result)

            # Write response to JSON file
            # from pathlib import Path
            # data_dir = Path(__file__).parent.parent.parent / 'data' / 'temp_debug'
            # data_dir.mkdir(exist_ok=True)
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # file_name = f"monte_carlo_simulation_{account_id}_{timestamp}.json"
            # file_path = data_dir / file_name

            # with open(file_path, 'w') as f:
            #     json.dump(processed_result, f, cls=CustomJSONEncoder, indent=2)

            # logger.info(f"Monte Carlo simulation result saved to {file_path}")

            # Cache the result
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO monte_carlo_cache (cache_key, result) VALUES (%s, %s) ON CONFLICT (cache_key) DO UPDATE SET result = EXCLUDED.result, created_at = CURRENT_TIMESTAMP",
                    (cache_key, json.dumps(processed_result, cls=CustomJSONEncoder))
                )
            conn.commit()

            # Update in-memory cache
            get_cached_simulation.cache_clear()
            get_cached_simulation(cache_key)

            # Use jsonable_encoder to prepare the response
            json_compatible_result = jsonable_encoder(processed_result)
            return JSONResponse(content=json_compatible_result)

    except HTTPException:
        raise
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
# TODO: Investigate if this is needed
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

@app.get("/tax-rates")
def get_tax_rates(current_user: str = Depends(get_current_user)):
    with get_db_connection() as conn:
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                # First, check if tax rates information exists for the user
                cur.execute("""
                    SELECT tax_rates_id, federal_income_tax_rate, state_income_tax_rate,
                           federal_long_term_capital_gains_rate, state_long_term_capital_gains_rate,
                           as_of_date
                    FROM tax_rates
                    WHERE user_id = %s
                    ORDER BY as_of_date DESC
                    LIMIT 1
                """, (current_user,))
                
                tax_rates = cur.fetchone()

                if not tax_rates:
                    raise HTTPException(
                        status_code=404,
                        detail="Tax rates information not found for this user"
                    )

                # Convert UUID to string for JSON serialization
                tax_rates['tax_rates_id'] = str(tax_rates['tax_rates_id'])

                return {
                    "status": "success",
                    "tax_rates": tax_rates
                }

        except psycopg.Error as e:
            logger.error(f"Database error while fetching tax rates: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        

# Add the following helper function to fetch tax rates
def fetch_user_tax_rates(user_id: str, target_date: date, conn: psycopg.Connection) -> Dict:
    with conn.cursor(row_factory=dict_row) as cur:
        # Fetch the most recent tax rates on or before the target_date
        cur.execute("""
            SELECT federal_income_tax_rate, state_income_tax_rate,
                   federal_long_term_capital_gains_rate, state_long_term_capital_gains_rate
            FROM tax_rates
            WHERE user_id = %s AND as_of_date <= %s
            ORDER BY as_of_date DESC
            LIMIT 1
        """, (user_id, target_date))
        tax_rates = cur.fetchone()
        if not tax_rates:
            raise HTTPException(status_code=404, detail="Tax rates not found for the user on or before the specified date")
        return tax_rates
    
# Define the Pydantic model for the request body
class OptimizeTaxesRequest(BaseModel):
    account_id: UUID4
    optimized_portfolio: Dict[str, Any]

@app.post("/optimize-taxes")
def optimize_taxes(
    account_id: UUID4,
    portfolio_id: UUID4,
    current_user: str = Depends(get_current_user)
):

    as_of_date = date.today()

    with get_db_connection() as conn:
        # Validate that the account and portfolio belong to the current user
        if not validate_account_id(str(account_id), current_user, conn):
            logger.warning(f"Account not found or doesn't belong to user. Account ID: {account_id}, User ID: {current_user}")
            raise HTTPException(
                status_code=404,
                detail="Account not found or doesn't belong to the current user"
            )

        if not validate_portfolio_id(str(portfolio_id), current_user, conn):
            logger.warning(f"Portfolio not found or doesn't belong to user. Portfolio ID: {portfolio_id}, User ID: {current_user}")
            raise HTTPException(
                status_code=404,
                detail="Portfolio not found or doesn't belong to the current user"
            )

        # Fetch the appropriate tax rates for the user
        tax_rates = fetch_user_tax_rates(current_user, as_of_date, conn)

        # Extract short-term and long-term federal tax rates
        short_term_tax_rate = tax_rates['federal_income_tax_rate']
        long_term_tax_rate = tax_rates['federal_long_term_capital_gains_rate']

        # Get the current holdings using the fetch_account_data function
        holdings = fetch_account_data(str(account_id), as_of_date.isoformat())

        if not holdings:
            logger.warning(f"No holdings found for account {account_id} on {as_of_date}")
            raise HTTPException(status_code=404, detail="No holdings found for this account on the latest date")

        # Get the optimized portfolio holdings using the fetch_portfolio_data function
        optimized_holdings = fetch_portfolio_data(as_of_date.isoformat(), portfolio_id=str(portfolio_id))

        if not optimized_holdings:
            logger.warning(f"No holdings found for portfolio {portfolio_id} on {as_of_date}")
            raise HTTPException(status_code=404, detail="No holdings found for this portfolio on the latest date")

        # Proceed with preparing the original_portfolio and optimized_portfolio data structures
        # The rest of the logic remains the same, using holdings and optimized_holdings
        # Prepare the original_portfolio data structure

        portfolio_by_symbol = defaultdict(list)
        for holding in holdings:
            portfolio_by_symbol[holding['symbol']].append(holding)

        original_portfolio = {}

        for symbol, symbol_holdings in portfolio_by_symbol.items():
            for holding in symbol_holdings:
                adjusted_cost_basis = holding['adjusted_cost_basis']
                purchase_date = holding.get('purchase_date')
                capital_gains_type = holding.get('capital_gains_type')
                as_of_date_holding = holding['as_of_date']
                security_type = holding['security_type']
                shares = holding['quantity']
                mstar_id = holding.get('mstar_id')
                current_price = holding.get('price', 0.0)

                total_value = shares * current_price
                account_balance = sum(h['quantity'] * h.get('price', 0.0) for h in holdings if h.get('price', 0.0) > 0)
                weight = total_value / account_balance if account_balance > 0 else 0.0

                # Convert dates to strings if necessary
                if isinstance(purchase_date, date):
                    purchase_date_str = purchase_date.isoformat()
                else:
                    purchase_date_str = purchase_date  # Assuming it's already a string or None

                if isinstance(as_of_date_holding, date):
                    as_of_date_holding_str = as_of_date_holding.isoformat()
                else:
                    as_of_date_holding_str = as_of_date_holding  # Assuming it's already a string

                # Unique identifier for each tax lot
                tax_lot_id = f"{symbol}_{purchase_date_str}_{adjusted_cost_basis}_{as_of_date_holding_str}"

                # Initialize the tax lot entry if it doesn't exist
                if tax_lot_id not in original_portfolio:
                    original_portfolio[tax_lot_id] = {
                        'symbol': symbol,
                        'weight': 0.0,  # Will be updated later
                        'security_type': security_type,
                        'current_price': current_price,
                        'current_price_as_of_date': as_of_date_holding_str,
                        'as_of_date': as_of_date.isoformat(),
                        'shares': [],
                        'adjusted_cost_basis': [],
                        'purchase_date': [],
                        'capital_gains_type': [],
                        'mstar_id': mstar_id
                    }

                # Append data to the lists
                original_portfolio[tax_lot_id]['shares'].append(shares)
                original_portfolio[tax_lot_id]['adjusted_cost_basis'].append(adjusted_cost_basis)
                original_portfolio[tax_lot_id]['purchase_date'].append(purchase_date_str)
                original_portfolio[tax_lot_id]['capital_gains_type'].append(capital_gains_type)

        # After building original_portfolio, calculate weights
        account_balance = sum(
            h['quantity'] * h.get('price', 0.0) for h in holdings if h.get('price', 0.0) > 0
        )

        for tax_lot_id, data in original_portfolio.items():
            total_value = sum(data['shares']) * data['current_price']
            weight = total_value / account_balance if account_balance > 0 else 0.0
            original_portfolio[tax_lot_id]['weight'] = weight

        # Prepare the optimized_portfolio data structure
        optimized_portfolio = {}

        for holding in optimized_holdings:
            symbol = holding['symbol']
            current_price = holding.get('price', 0.0)
            shares = holding.get('quantity', 0.0)
            security_type = holding.get('security_type', 'Unknown')
            mstar_id = holding.get('mstar_id')
            adjusted_cost_basis = holding.get('adjusted_cost_basis', current_price)

            total_value = shares * current_price
            optimized_account_balance = sum(
                h['quantity'] * h.get('price', 0.0) for h in optimized_holdings if h.get('price', 0.0) > 0
            )
            weight = total_value / optimized_account_balance if optimized_account_balance > 0 else 0.0

            optimized_portfolio[symbol] = {
                'symbol': symbol,
                'weight': weight,
                'security_type': security_type,
                'current_price': current_price,
                'current_price_as_of_date': as_of_date.isoformat(),
                'as_of_date': as_of_date.isoformat(),
                'shares': shares,
                'mstar_id': mstar_id,
                'adjusted_cost_basis': adjusted_cost_basis  # Optional, for new purchases
            }

        try:
            # Call the tax_optimized_rebalance function with the prepared data and tax rates
            result = tax_optimized_rebalance(
                optimized_portfolio=optimized_portfolio,
                original_portfolio=original_portfolio,
                short_term_tax_rate=short_term_tax_rate,
                long_term_tax_rate=long_term_tax_rate
            )

            # Return the result with a success status
            logger.info(f"Successfully optimized taxes for account {account_id} and portfolio {portfolio_id}")
            return JSONResponse(content=result)

        except ValueError as e:
            # Handle expected errors from input validation
            logger.error(f"ValueError during tax optimized rebalance: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        except HTTPException as http_exc:
            logger.exception(f"HTTPException occurred: {http_exc.detail}")
            raise http_exc
        except Exception as e:
            # Log unexpected errors and return a generic internal server error
            logger.exception(f"Unexpected error during tax optimized rebalance: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

class PortfolioHolding(BaseModel):
       symbol: str
       quantity: float
       as_of_date: Optional[date] = None
       security_type: Optional[str] = None
       cusip: Optional[str] = None
       mstar_id: Optional[str] = None

       @validator("quantity")
       def quantity_must_be_positive(cls, v):
           if v <= 0:
               raise ValueError("Quantity must be greater than 0")
           return v

class PortfolioHoldingsCreate(BaseModel):
    account_id: UUID4
    holdings: List[PortfolioHolding]

@app.post("/add-portfolio-holdings")
def add_portfolio_holdings(
    holdings_data: PortfolioHoldingsCreate,
    current_user: str = Depends(get_current_user)
):
    # logger.info(f"Received request to add holdings for a new portfolio.")
    # logger.info(f"Holdings data: {holdings_data.model_dump()}")

    with get_db_connection() as conn:
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                # Generate a new portfolio_id
                new_portfolio_id = uuid4()
                logger.debug(f"Generated new portfolio_id: {new_portfolio_id}")

                # Proceed with holdings validation
                logger.debug("Proceeding with holdings validation")

                # Validate symbols against security_master and adjust as_of_date
                for holding in holdings_data.holdings:
                    logger.debug(f"Validating holding: {holding.model_dump()}")

                    # Set default as_of_date if not provided
                    holding_as_of_date = holding.as_of_date or date.today()

                    query = """
                    SELECT 
                        symbol, 
                        security_type, 
                        cusip, 
                        mstar_id,
                        as_of_date
                    FROM security_master
                    WHERE symbol = %s
                    """
                    params = [holding.symbol]

                    if holding.security_type:
                        query += " AND security_type = %s"
                        params.append(holding.security_type)
                    if holding.cusip:
                        query += " AND cusip = %s"
                        params.append(holding.cusip)

                    query += " AND as_of_date <= %s"
                    params.append(holding_as_of_date)

                    query += " ORDER BY as_of_date DESC LIMIT 1"

                    logger.debug(f"Executing query: {query} with params: {params}")
                    cur.execute(query, params)
                    security = cur.fetchone()

                    if not security:
                        logger.warning(f"Invalid symbol, security_type, or cusip combination or no matching as_of_date for symbol: {holding.symbol}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid symbol, security_type, or cusip combination or no matching as_of_date for symbol: {holding.symbol}"
                        )

                    logger.debug(f"Security found: {security}")
                    # Update holding with fetched data
                    holding.security_type = security['security_type']
                    holding.cusip = security.get('cusip')
                    if 'mstar_id' in security and security['mstar_id']:
                        holding.mstar_id = security['mstar_id']
                    # Use the as_of_date from security_master to satisfy foreign key constraint
                    holding.as_of_date = security['as_of_date']

                logger.debug("All holdings validated, preparing for insertion")

                # Prepare the query for inserting multiple holdings
                insert_query = """
                    INSERT INTO portfolio_holdings 
                    (symbol, quantity, as_of_date, created_at, account_id, 
                     security_type, cusip, mstar_id, portfolio_id, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Prepare the data for bulk insert
                insert_data = [
                    (
                        holding.symbol,
                        holding.quantity,
                        holding.as_of_date,
                        datetime.now(timezone.utc),
                        holdings_data.account_id,
                        holding.security_type,
                        holding.cusip,
                        holding.mstar_id,
                        new_portfolio_id,
                        current_user
                    )
                    for holding in holdings_data.holdings
                ]

                logger.debug(f"Executing bulk insert with {len(insert_data)} holdings")
                logger.debug(f"Insert query: {insert_query}")
                logger.debug(f"Insert data: {insert_data}")

                try:
                    # Execute the bulk insert
                    cur.executemany(insert_query, insert_data)
                except psycopg.errors.ForeignKeyViolation as e:
                    logger.error(f"Foreign key violation during bulk insert: {str(e)}")
                    raise HTTPException(status_code=400, detail=f"Foreign key violation: {str(e)}")
                except psycopg.Error as e:
                    logger.error(f"Database error during bulk insert: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Database error during bulk insert: {str(e)}")

            conn.commit()
            logger.info(f"Successfully added {len(holdings_data.holdings)} holdings to new portfolio {new_portfolio_id}")
        except HTTPException as http_exc:
            logger.exception("HTTP exception occurred")
            raise http_exc
        except psycopg_errors.ForeignKeyViolation:
            raise HTTPException(status_code=400, detail="Invalid account ID")
        except psycopg_errors.CheckViolation as e:
            raise HTTPException(status_code=400, detail=f"Constraint violation: {str(e)}")
        except psycopg_errors.UniqueViolation:
            raise HTTPException(
                status_code=409, detail="Duplicate entry for portfolio holdings"
            )
        except psycopg_errors.NotNullViolation as e:
            raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")
        except psycopg.Error as e:
            conn.rollback()
            logger.exception(f"Database error while adding portfolio holdings: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    return {
        "status": "success",
        "message": f"Added {len(holdings_data.holdings)} holdings to the new portfolio",
        "portfolio_id": new_portfolio_id  # Return the generated portfolio_id in the response
    }
class PortfolioHolding(BaseModel):
    symbol: str
    quantity: float
    as_of_date: date
    created_at: date
    account_id: UUID
    security_type: str
    cusip: str
    mstar_id: str
    portfolio_id: UUID
    # Add any additional fields as necessary

class GetPortfolioHoldingsResponse(BaseModel):
    status: str
    holdings: List[PortfolioHolding]
    
@app.get("/get-portfolio-holdings", response_model=GetPortfolioHoldingsResponse)
def get_portfolio_holdings(
    portfolio_id: Optional[UUID] = Query(None, description="The UUID of the portfolio to fetch holdings for"),
    account_id: Optional[UUID] = Query(None, description="The UUID of the account associated with the portfolio"),
    as_of_date: datetime = Query(..., description="The date for which to fetch portfolio holdings in YYYY-MM-DD format"),
    current_user: str = Depends(get_current_user)
):
    """
    Retrieves the holdings of a specific portfolio as of a given date.
    Optionally filters by account_id.

    Args:
        portfolio_id (Optional[UUID]): The UUID of the portfolio.
        account_id (Optional[UUID]): The UUID of the account.
        as_of_date (datetime): The date for which to retrieve the holdings.
        current_user (str): The currently authenticated user.

    Returns:
        GetPortfolioHoldingsResponse: A response model containing the status and list of holdings.
    """
    logger.info(f"User {current_user} requested holdings for portfolio {portfolio_id} and account {account_id} as of {as_of_date.date()}")
    
    if not portfolio_id and not account_id:
        logger.error("Both portfolio_id and account_id are missing in the request.")
        raise HTTPException(
            status_code=400,
            detail="At least one of portfolio_id or account_id must be provided."
        )
    
    try:
        holdings = fetch_portfolio_data(
            as_of_date=as_of_date.strftime("%Y-%m-%d"),
            portfolio_id=str(portfolio_id) if portfolio_id else None,
            account_id=str(account_id) if account_id else None
        )

        if not holdings:
            logger.warning(f"No holdings found for portfolio_id {portfolio_id} and account_id {account_id} on or before {as_of_date.date()}.")
            raise HTTPException(
                status_code=404,
                detail=f"No holdings found for the specified portfolio and/or account on or before {as_of_date.date()}."
            )

        # Convert holdings to PortfolioHolding models
        portfolio_holdings = [PortfolioHolding(**holding) for holding in holdings]

        return GetPortfolioHoldingsResponse(
            status="success",
            holdings=portfolio_holdings
        )

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise HTTPException(
            status_code=400,
            detail=str(ve)
        )
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions
        logger.exception(f"HTTPException occurred: {http_exc.detail}")
        raise http_exc
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching portfolio holdings."
        )
    
@app.get("/asset-allocation")
def get_asset_allocation(
    ids: List[UUID] = Query(..., description="List of portfolio or account IDs."),
    types: List[str] = Query(..., description="List of types corresponding to each ID ('account' or 'portfolio')."),
    as_of_date: Optional[datetime] = Query(
        None,
        description="As of date in 'YYYY-MM-DD' format. Defaults to today's date if not provided."
    ),
    current_user: str = Depends(get_current_user)
) -> Dict[str, Dict[str, float]]:
    """
    Retrieves the asset allocation for a list of portfolios or accounts as of a given date.

    Args:
        ids (List[UUID]): A list of portfolio or account IDs.
        types (List[str]): Corresponding types for each ID ('account' or 'portfolio').
        as_of_date (Optional[datetime]): The date for which to retrieve the asset allocations. Defaults to today's date if not provided.
        current_user (str): The currently authenticated user.

    Returns:
        Dict[str, Dict[str, float]]: A dictionary where keys are IDs and values are asset allocation objects.
    """
    # If as_of_date is not provided, default to today's date
    if as_of_date is None:
        as_of_date = datetime.today()

    logger.info(f"User {current_user} requested asset allocation for IDs {ids} with types {types} as of {as_of_date.date()}.")

    if len(ids) != len(types):
        logger.error("The number of IDs and types provided do not match.")
        raise HTTPException(
            status_code=400,
            detail="The number of IDs and types must be the same."
        )

    valid_types = {'account', 'portfolio'}
    allocations = {}

    for idx, id_value in enumerate(ids):
        id_str = str(id_value)
        type_value = types[idx].lower()

        if type_value not in valid_types:
            logger.error(f"Invalid type '{type_value}' provided for ID '{id_str}'.")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid type '{type_value}' for ID '{id_str}'. Allowed types are 'account' or 'portfolio'."
            )

        try:
            # Calculate asset allocation using the utility function
            allocation = calculate_asset_allocation(
                as_of_date=as_of_date.strftime("%Y-%m-%d"),
                argument_type=type_value,
                argument_id=id_str
            )
            allocations[id_str] = allocation

        except ValueError as ve:
            logger.error(f"ValueError for ID '{id_str}': {ve}")
            # Include the ID in the error detail
            raise HTTPException(
                status_code=400,
                detail=f"Error for ID '{id_str}': {ve}"
            )
        except Exception as e:
            logger.exception(f"An unexpected error occurred for ID '{id_str}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"An unexpected error occurred while processing ID '{id_str}'."
            )
        
    # Write allocations to JSON file
    json_filename = f"asset_allocations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    json_filepath = os.path.join("data", "asset_allocations", json_filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(json_filepath), exist_ok=True)

    # Write the JSON file
    with open(json_filepath, 'w') as json_file:
        json.dump(allocations, json_file, cls=CustomJSONEncoder, indent=2)

    logger.info(f"Asset allocations written to {json_filepath}")

    # Return the allocations as a JSON response
    return allocations

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
