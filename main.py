import json
import os
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
API_PORT   = int(os.environ.get("API_PORT", 8080))
API_TOKEN  = os.environ.get("API_TOKEN", "f118da769ab686ae753192d548f67a3a371904b9d805f4c8fef293cda884fc7f")

app  = FastAPI(title="MT5 Poller API")
pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
bearer = HTTPBearer()


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


def verify_token(credentials: HTTPAuthorizationCredentials = Security(bearer)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


# =============================================================================
# Pydantic models
# =============================================================================

class Position(BaseModel):
    ticket: int
    login: int
    symbol: str
    cmd: int                # 0=BUY 1=SELL
    volume: float
    open_price: float
    price_current: float
    open_time: int          # Unix timestamp
    pnl: float
    swap: float
    notional_value: float
    contract_size: float
    profit_currency: str


class PositionsPayload(BaseModel):
    positions: list[Position]

    @field_validator("positions")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("positions array must not be empty")
        return v


class Account(BaseModel):
    # --- identity ---
    login: int
    group: str
    name: str
    first_name: str = ""
    last_name: str = ""
    middle_name: str = ""
    email: str = ""
    phone: str = ""
    country: str = ""
    city: str = ""
    state: str = ""
    address: str = ""
    zip_code: str = ""
    company: str = ""
    crm_id: str = ""        # stored in Comment field
    external_id: str = ""
    status: str = ""
    lead_campaign: str = ""
    lead_source: str = ""
    # --- account settings ---
    leverage: int = 0
    rights: int = 0
    agent: int = 0
    registration: int = 0   # Unix timestamp
    last_access: int = 0    # Unix timestamp
    limit_orders: int = 0
    # --- static financials (from IMTUser, recorded balance) ---
    balance_book: float = 0.0
    credit: float = 0.0
    interest_rate: float = 0.0
    commission_daily: float = 0.0
    commission_monthly: float = 0.0
    balance_prev_day: float = 0.0
    balance_prev_month: float = 0.0
    equity_prev_day: float = 0.0
    equity_prev_month: float = 0.0
    # --- real-time financials (from IMTAccount) ---
    balance: float = 0.0
    equity: float = 0.0
    margin: float = 0.0
    margin_free: float = 0.0
    margin_level: float = 0.0
    margin_leverage: int = 0
    profit: float = 0.0
    floating: float = 0.0
    storage: float = 0.0
    blocked_commission: float = 0.0
    blocked_profit: float = 0.0
    margin_initial: float = 0.0
    margin_maintenance: float = 0.0
    assets: float = 0.0
    liabilities: float = 0.0
    currency_digits: int = 2
    # --- risk / stop-out ---
    so_activation: int = 0  # 0=none 1=margin_call 2=stop_out
    so_time: int = 0
    so_level: float = 0.0
    so_equity: float = 0.0
    so_margin: float = 0.0


class AccountsPayload(BaseModel):
    accounts: list[Account]

    @field_validator("accounts")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("accounts array must not be empty")
        return v


# =============================================================================
# Health
# =============================================================================

@app.get("/health", dependencies=[Security(verify_token)])
def health():
    try:
        get_redis().ping()
        redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {e}"
    return {"api": "ok", "redis": redis_status}


# =============================================================================
# Positions endpoints
# =============================================================================

@app.post("/positions", dependencies=[Security(verify_token)])
def post_positions(payload: PositionsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set("positions:latest:raw", payload.model_dump_json())
    pipe.set("positions:last_update", now)
    for pos in payload.positions:
        pipe.set(f"position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.sadd("positions:tickets", str(pos.ticket))
    pipe.execute()

    return {
        "success": True,
        "positions_processed": len(payload.positions),
        "tickets": [p.ticket for p in payload.positions],
    }


@app.get("/positions/latest", dependencies=[Security(verify_token)])
def get_positions_latest():
    r = get_redis()
    raw = r.get("positions:latest:raw")
    if raw is None:
        raise HTTPException(status_code=404, detail="No positions stored yet")
    tickets = list(r.smembers("positions:tickets"))
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"position:{t}")
    individual = {t: json.loads(d) for t, d in zip(tickets, pipe.execute()) if d}
    return {
        "last_update": r.get("positions:last_update"),
        "raw_payload": json.loads(raw),
        "positions": individual,
    }


@app.get("/positions", dependencies=[Security(verify_token)])
def get_all_positions():
    r = get_redis()
    tickets = list(r.smembers("positions:tickets"))
    if not tickets:
        return {"count": 0, "positions": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"position:{t}")
    positions = sorted(
        [json.loads(d) for d in pipe.execute() if d],
        key=lambda p: p["ticket"],
    )
    return {"count": len(positions), "positions": positions}


# =============================================================================
# Accounts endpoints
# =============================================================================

@app.post("/accounts", dependencies=[Security(verify_token)])
def post_accounts(payload: AccountsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set("accounts:latest:raw", payload.model_dump_json())
    pipe.set("accounts:last_update", now)
    for acct in payload.accounts:
        pipe.set(f"account:{acct.login}", json.dumps(acct.model_dump()))
        pipe.sadd("accounts:logins", str(acct.login))
    pipe.execute()

    return {
        "success": True,
        "accounts_processed": len(payload.accounts),
        "logins": [a.login for a in payload.accounts],
    }


@app.get("/accounts/latest", dependencies=[Security(verify_token)])
def get_accounts_latest():
    r = get_redis()
    raw = r.get("accounts:latest:raw")
    if raw is None:
        raise HTTPException(status_code=404, detail="No accounts stored yet")
    logins = list(r.smembers("accounts:logins"))
    pipe = r.pipeline()
    for lg in logins:
        pipe.get(f"account:{lg}")
    individual = {lg: json.loads(d) for lg, d in zip(logins, pipe.execute()) if d}
    return {
        "last_update": r.get("accounts:last_update"),
        "raw_payload": json.loads(raw),
        "accounts": individual,
    }


@app.get("/accounts", dependencies=[Security(verify_token)])
def get_all_accounts():
    r = get_redis()
    logins = list(r.smembers("accounts:logins"))
    if not logins:
        return {"count": 0, "accounts": []}
    pipe = r.pipeline()
    for lg in logins:
        pipe.get(f"account:{lg}")
    accounts = sorted(
        [json.loads(d) for d in pipe.execute() if d],
        key=lambda a: a["login"],
    )
    return {"count": len(accounts), "accounts": accounts}


@app.get("/accounts/{login}", dependencies=[Security(verify_token)])
def get_account(login: int):
    r = get_redis()
    data = r.get(f"account:{login}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Account {login} not found")
    return json.loads(data)
