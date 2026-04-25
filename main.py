import json
import os
from datetime import datetime, timezone

import msgpack
import redis
from fastapi import FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator
from starlette.middleware.base import BaseHTTPMiddleware

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
API_PORT   = int(os.environ.get("API_PORT", 8080))
API_TOKEN  = os.environ.get("API_TOKEN", "f118da769ab686ae753192d548f67a3a371904b9d805f4c8fef293cda884fc7f")

# How many historical rate entries to keep per symbol (1440 = 24 h at 1-min intervals)
RATE_HISTORY_MAX = 1440

app = FastAPI(title="MT5 Poller API")


# ---------------------------------------------------------------------------
# Middleware — transparent msgpack → JSON conversion for all endpoints
# ---------------------------------------------------------------------------

class MsgpackMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if "msgpack" in request.headers.get("content-type", ""):
            try:
                body = await request.body()
                decoded = msgpack.unpackb(body, raw=False)
                json_body = json.dumps(decoded).encode()
                async def receive():
                    return {"type": "http.request", "body": json_body, "more_body": False}
                request._receive = receive
            except Exception as exc:
                return JSONResponse({"error": f"msgpack decode failed: {exc}"}, status_code=400)
        return await call_next(request)

app.add_middleware(MsgpackMiddleware)

pool   = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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
    cmd: int
    volume: float
    open_price: float
    price_current: float
    open_time: int
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


class ClosedPosition(BaseModel):
    ticket: int           # closing deal ticket
    position_id: int
    login: int
    symbol: str
    action: int           # original direction (0=buy 1=sell)
    entry: int            # 1=OUT 2=INOUT 3=OUT_BY
    price: float          # close price
    volume_lots: float
    profit: float
    commission: float
    swap: float
    fee: float
    open_time: int = 0    # when position was originally opened (0 if unknown)
    close_time: int       # when position was closed
    comment: str = ""


class ClosedPositionsPayload(BaseModel):
    closed_positions: list[ClosedPosition]

    @field_validator("closed_positions")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("closed_positions array must not be empty")
        return v


class Account(BaseModel):
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
    crm_id: str = ""
    external_id: str = ""
    status: str = ""
    lead_campaign: str = ""
    lead_source: str = ""
    leverage: int = 0
    rights: int = 0
    agent: int = 0
    registration: int = 0
    last_access: int = 0
    limit_orders: int = 0
    balance_book: float = 0.0
    credit: float = 0.0
    interest_rate: float = 0.0
    commission_daily: float = 0.0
    commission_monthly: float = 0.0
    balance_prev_day: float = 0.0
    balance_prev_month: float = 0.0
    equity_prev_day: float = 0.0
    equity_prev_month: float = 0.0
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
    so_activation: int = 0
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


class Deal(BaseModel):
    ticket: int
    order: int
    position_id: int
    login: int
    symbol: str
    action: int
    entry: int
    price: float
    price_sl: float
    price_tp: float
    volume_lots: float
    volume_closed_lots: float
    profit: float
    commission: float
    swap: float
    fee: float
    reason: int
    time: int
    time_msc: int
    comment: str = ""


class DealsPayload(BaseModel):
    deals: list[Deal]

    @field_validator("deals")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("deals array must not be empty")
        return v


class Rate(BaseModel):
    symbol: str
    bid: float
    ask: float
    time: int             # unix timestamp (seconds)


class RatesPayload(BaseModel):
    rates: list[Rate]

    @field_validator("rates")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("rates array must not be empty")
        return v


# =============================================================================
# Health & Stats
# =============================================================================

@app.get("/health", dependencies=[Security(verify_token)])
def health():
    try:
        get_redis().ping()
        redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {e}"
    return {"api": "ok", "redis": redis_status}


@app.get("/stats", dependencies=[Security(verify_token)])
def stats():
    r = get_redis()
    pipe = r.pipeline()
    pipe.scard("positions:tickets")
    pipe.scard("accounts:logins")
    pipe.scard("deals:tickets")
    pipe.scard("closed_positions:tickets")
    pipe.scard("rates:symbols")
    pipe.get("positions:last_update")
    pipe.get("accounts:last_update")
    pipe.get("deals:last_update")
    pipe.get("closed_positions:last_update")
    pipe.get("rates:last_update")
    pos, acct, deal, cpx, rate, pts, ats, dts, cpts, rts = pipe.execute()
    return {
        "positions":         {"count": pos,  "last_update": pts},
        "accounts":          {"count": acct, "last_update": ats},
        "deals":             {"count": deal, "last_update": dts},
        "closed_positions":  {"count": cpx,  "last_update": cpts},
        "rates":             {"count": rate, "last_update": rts},
    }


# =============================================================================
# Positions
# =============================================================================

@app.post("/positions", dependencies=[Security(verify_token)])
def post_positions(payload: PositionsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    new_tickets = {str(p.ticket) for p in payload.positions}
    old_tickets = r.smembers("positions:tickets") or set()
    stale = old_tickets - new_tickets

    pipe = r.pipeline()
    for t in stale:
        pipe.delete(f"position:{t}")
    pipe.delete("positions:tickets")
    pipe.set("positions:latest:raw", payload.model_dump_json())
    pipe.set("positions:last_update", now)
    for pos in payload.positions:
        pipe.set(f"position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.sadd("positions:tickets", str(pos.ticket))
    pipe.execute()

    return {"success": True, "positions_processed": len(payload.positions),
            "tickets": [p.ticket for p in payload.positions], "stale_removed": len(stale)}


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
    return {"last_update": r.get("positions:last_update"), "raw_payload": json.loads(raw),
            "positions": individual}


@app.get("/positions", dependencies=[Security(verify_token)])
def get_all_positions():
    r = get_redis()
    tickets = list(r.smembers("positions:tickets"))
    if not tickets:
        return {"count": 0, "positions": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"position:{t}")
    positions = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda p: p["ticket"])
    return {"count": len(positions), "positions": positions}


# =============================================================================
# Closed Positions (monthly closing deals)
# =============================================================================

@app.post("/closed_positions/reset", dependencies=[Security(verify_token)])
def reset_closed_positions():
    """Clear all closed position data — call before the first chunk of a new push cycle."""
    r = get_redis()
    tickets = r.smembers("closed_positions:tickets") or set()
    pipe = r.pipeline()
    for t in tickets:
        pipe.delete(f"closed_position:{t}")
    pipe.delete("closed_positions:tickets")
    pipe.delete("closed_positions:last_update")
    pipe.execute()
    return {"success": True, "cleared": len(tickets)}


@app.post("/closed_positions", dependencies=[Security(verify_token)])
def post_closed_positions(payload: ClosedPositionsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set("closed_positions:last_update", now)
    for cp in payload.closed_positions:
        pipe.set(f"closed_position:{cp.ticket}", json.dumps(cp.model_dump()))
        pipe.sadd("closed_positions:tickets", str(cp.ticket))
    pipe.execute()

    return {"success": True, "closed_positions_processed": len(payload.closed_positions),
            "tickets": [cp.ticket for cp in payload.closed_positions]}


@app.get("/closed_positions", dependencies=[Security(verify_token)])
def get_closed_positions():
    r = get_redis()
    tickets = list(r.smembers("closed_positions:tickets"))
    if not tickets:
        return {"count": 0, "closed_positions": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"closed_position:{t}")
    records = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda x: x["time"], reverse=True)
    return {"count": len(records), "closed_positions": records}


@app.get("/closed_positions/{ticket}", dependencies=[Security(verify_token)])
def get_closed_position(ticket: int):
    r = get_redis()
    data = r.get(f"closed_position:{ticket}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Closed position {ticket} not found")
    return json.loads(data)


# =============================================================================
# Accounts
# =============================================================================

@app.post("/accounts/reset", dependencies=[Security(verify_token)])
def reset_accounts():
    """Clear all account data — call before the first chunk of a new push cycle."""
    r = get_redis()
    logins = r.smembers("accounts:logins") or set()
    pipe = r.pipeline()
    for lg in logins:
        pipe.delete(f"account:{lg}")
    pipe.delete("accounts:logins")
    pipe.delete("accounts:last_update")
    pipe.execute()
    return {"success": True, "cleared": len(logins)}


@app.post("/accounts", dependencies=[Security(verify_token)])
def post_accounts(payload: AccountsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    pipe.set("accounts:last_update", now)
    for acct in payload.accounts:
        pipe.set(f"account:{acct.login}", json.dumps(acct.model_dump()))
        pipe.sadd("accounts:logins", str(acct.login))
    pipe.execute()
    return {"success": True, "accounts_processed": len(payload.accounts),
            "logins": [a.login for a in payload.accounts]}


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
    return {"last_update": r.get("accounts:last_update"), "raw_payload": json.loads(raw),
            "accounts": individual}


@app.get("/accounts", dependencies=[Security(verify_token)])
def get_all_accounts():
    r = get_redis()
    logins = list(r.smembers("accounts:logins"))
    if not logins:
        return {"count": 0, "accounts": []}
    pipe = r.pipeline()
    for lg in logins:
        pipe.get(f"account:{lg}")
    accounts = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda a: a["login"])
    return {"count": len(accounts), "accounts": accounts}


@app.get("/accounts/{login}", dependencies=[Security(verify_token)])
def get_account(login: int):
    r = get_redis()
    data = r.get(f"account:{login}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Account {login} not found")
    return json.loads(data)


# =============================================================================
# Deals
# =============================================================================

@app.post("/deals", dependencies=[Security(verify_token)])
def post_deals(payload: DealsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    pipe.set("deals:last_update", now)
    for deal in payload.deals:
        pipe.set(f"deal:{deal.ticket}", json.dumps(deal.model_dump()))
        pipe.sadd("deals:tickets", str(deal.ticket))
    pipe.execute()
    return {"success": True, "deals_processed": len(payload.deals),
            "tickets": [d.ticket for d in payload.deals]}


@app.get("/deals", dependencies=[Security(verify_token)])
def get_all_deals():
    r = get_redis()
    tickets = list(r.smembers("deals:tickets"))
    if not tickets:
        return {"count": 0, "deals": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deal:{t}")
    deals = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda d: d["time"], reverse=True)
    return {"count": len(deals), "deals": deals}


@app.get("/deals/latest", dependencies=[Security(verify_token)])
def get_deals_latest():
    r = get_redis()
    last_update = r.get("deals:last_update")
    if last_update is None:
        raise HTTPException(status_code=404, detail="No deals stored yet")
    tickets = list(r.smembers("deals:tickets"))
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deal:{t}")
    deals = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda d: d["time"], reverse=True)
    return {"last_update": last_update, "count": len(deals), "deals": deals[:100]}


@app.get("/deals/{ticket}", dependencies=[Security(verify_token)])
def get_deal(ticket: int):
    r = get_redis()
    data = r.get(f"deal:{ticket}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Deal {ticket} not found")
    return json.loads(data)


# =============================================================================
# Conversion Rates (with history)
# =============================================================================

@app.post("/rates/reset", dependencies=[Security(verify_token)])
def reset_rates():
    """Clear all rate data and history — call before posting a new rates snapshot."""
    r = get_redis()
    symbols = r.smembers("rates:symbols") or set()
    pipe = r.pipeline()
    for s in symbols:
        pipe.delete(f"rate:{s}")
        pipe.delete(f"rate:{s}:history")
    pipe.delete("rates:symbols")
    pipe.delete("rates:last_update")
    pipe.execute()
    return {"success": True, "cleared": len(symbols)}


@app.post("/rates", dependencies=[Security(verify_token)])
def post_rates(payload: RatesPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set("rates:last_update", now)
    for rate in payload.rates:
        entry = json.dumps(rate.model_dump())
        pipe.set(f"rate:{rate.symbol}", entry)
        pipe.sadd("rates:symbols", rate.symbol)
        # Append to per-symbol history ring, keep last 1440 entries (24 h at 1 min)
        pipe.rpush(f"rate:{rate.symbol}:history", entry)
        pipe.ltrim(f"rate:{rate.symbol}:history", -RATE_HISTORY_MAX, -1)
    pipe.execute()

    return {"success": True, "rates_processed": len(payload.rates),
            "symbols": [r.symbol for r in payload.rates]}


@app.get("/rates", dependencies=[Security(verify_token)])
def get_rates():
    r = get_redis()
    symbols = list(r.smembers("rates:symbols"))
    if not symbols:
        return {"count": 0, "rates": [], "last_update": None}
    pipe = r.pipeline()
    for s in symbols:
        pipe.get(f"rate:{s}")
    rates = sorted([json.loads(d) for d in pipe.execute() if d], key=lambda x: x["symbol"])
    return {"count": len(rates), "rates": rates, "last_update": r.get("rates:last_update")}


@app.get("/rates/history/{symbol}", dependencies=[Security(verify_token)])
def get_rate_history(symbol: str, limit: int = 60):
    r = get_redis()
    raw_list = r.lrange(f"rate:{symbol}:history", -min(limit, RATE_HISTORY_MAX), -1)
    if not raw_list:
        raise HTTPException(status_code=404, detail=f"No history for {symbol}")
    history = [json.loads(e) for e in raw_list]
    return {"symbol": symbol, "count": len(history), "history": history}
