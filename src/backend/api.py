from fastapi import FastAPI, Depends, HTTPException, Security, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from pydantic import BaseModel, UUID4, validator, Field, EmailStr
from typing import Dict, List, Optional
import requests
import os
import psycopg
from dotenv import load_dotenv
from datetime import date, datetime
from uuid import uuid4
from psycopg import errors as psycopg_errors
import logging
from urllib.parse import urlparse
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from src.domain_logic import IncomeTaxRates
from src.utils import validate_payload
from src.domain_logic.simulation import simulate_balance_paths
from src.utils.calcs import portfolio_statistics

logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

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
def get_db_connection():
    conn = psycopg.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    try:
        yield conn
    finally:
        conn.close()


@app.post("/create-account")
async def create_account(
    account: AccountCreate,
    current_user: str = Depends(get_current_user),
    conn: psycopg.Connection = Depends(get_db_connection),
):
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
async def add_account_holdings(
    holdings_data: AccountHoldingsCreate,
    current_user: str = Depends(get_current_user),
    conn: psycopg.Connection = Depends(get_db_connection),
):
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
async def calculate_tax_rates(
    tax_info: dict,
    current_user: str = Depends(get_current_user),
    conn: psycopg.Connection = Depends(get_db_connection),
):
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
async def add_user(
    user: UserCreate,
    conn: psycopg.Connection = Depends(get_db_connection),
):
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

async def fetch_account_statistics(
    account_id: UUID4,
    current_user: str,
    conn: psycopg.Connection
) -> dict:
    try:
        # Check if the account belongs to the current user
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT 1 FROM accounts WHERE account_id = %s AND user_id = %s",
                (account_id, current_user)
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail="Account not found or doesn't belong to the current user"
                )

            # Fetch account holdings
            cur.execute(
                """
                SELECT symbol, quantity, adjusted_cost_basis
                FROM account_holdings
                WHERE account_id = %s
                """,
                (account_id,)
            )
            holdings = cur.fetchall()

        if not holdings:
            raise HTTPException(
                status_code=404,
                detail="No holdings found for this account"
            )

        # Calculate portfolio statistics
        stats = portfolio_statistics(holdings)

        return {
            "account_id": account_id,
            "balance": stats["total_value"],
            "annual_mean_return": stats["annual_return"],
            "annual_std_dev": stats["annual_volatility"],
            "sharpe_ratio": stats["sharpe_ratio"],
            "asset_allocation": stats["asset_allocation"]
        }

    except psycopg.Error as e:
        logger.error(f"Database error while fetching account statistics: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        logger.error(f"Error while calculating account statistics: {str(e)}")
        raise HTTPException(status_code=500, detail="Error calculating account statistics")

@app.get("/account-statistics/{account_id}")
async def get_account_statistics(
    account_id: UUID4,
    current_user: str = Depends(get_current_user),
    conn: psycopg.Connection = Depends(get_db_connection)
):
    return await fetch_account_statistics(account_id, current_user, conn)

@app.get("/monte-carlo-simulation")
async def monte_carlo_simulation(
    account_id: str,
    percentiles: Optional[List[float]] = Query(default=[25, 50, 75], description="List of percentiles to calculate"),
    current_user: str = Depends(get_current_user),
    conn: psycopg.Connection = Depends(get_db_connection)
):
    try:
        # Validate percentiles
        if not all(0 <= p <= 100 for p in percentiles):
            raise HTTPException(status_code=400, detail="Percentiles must be between 0 and 100")

        # Fetch account details
        account_stats = await fetch_account_statistics(account_id, current_user, conn)

        # Simulate balance paths
        start_date = date.today().isoformat()
        simulation_result = simulate_balance_paths(
            starting_balance=account_stats["account_balance"],
            annual_mean=account_stats["return"],
            annual_std_dev=account_stats["volatility"],
            start_date=start_date,
            lower_percentile=min(percentiles),
            upper_percentile=max(percentiles)
        )

        # Extract paths for requested percentiles
        percentile_paths = {
            f"percentile_{p}": simulation_result["balance_paths"].quantile(p/100, axis=1).to_dict()
            for p in percentiles
        }

        # Prepare response
        response = {
            "account_id": account_id,
            "account_info": account_stats,
            "simulation_start_date": start_date,
            "percentile_paths": percentile_paths,
            "final_balance_min": float(simulation_result["final_balance_min"]),
            "final_balance_max": float(simulation_result["final_balance_max"]),
            "final_mean_balance": float(simulation_result["final_mean_balance"]),
        }

        return response

    except psycopg.Error as e:
        logger.error(f"Database error during Monte Carlo simulation: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    except Exception as e:
        logger.error(f"Error during Monte Carlo simulation: {str(e)}")
        raise HTTPException(status_code=500, detail="Error during simulation")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
