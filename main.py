import json
import os
import uuid
from datetime import date, datetime, timezone

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


def _delete_keys(r: redis.Redis, keys: list[str]):
    if not keys:
        return
    for i in range(0, len(keys), 1000):
        r.delete(*keys[i : i + 1000])


def _scan_keys(r: redis.Redis, match: str) -> list[str]:
    cursor = 0
    keys: list[str] = []
    while True:
        cursor, batch = r.scan(cursor, match=match, count=500)
        keys.extend(batch)
        if cursor == 0:
            return keys


def _migrate_synced_index(r: redis.Redis, prefix: str, synced_set: str, marker_key: str):
    """One-shot backfill of `*:months:synced` from existing `*:month:*` keys.
    Marker-gated so the SCAN runs at most once per Redis instance."""
    if r.get(marker_key):
        return
    keys = _scan_keys(r, f"{prefix}:month:*")
    months = {k.replace(f"{prefix}:month:", "") for k in keys}
    if months:
        r.sadd(synced_set, *months)
    r.set(marker_key, "1")
    print(f"[startup] {synced_set}: backfilled {len(months)} months")


@app.on_event("startup")
def migrate_synced_indices():
    """Populate closed_positions/deals synced indices from any orphan month keys.
    Runs once per Redis instance (gated by *:synced_migrated marker).

    Also backfills the per-month index for deposits_withdrawals from the
    existing flat tickets SET, since the original feature stored everything
    flat — without this the first new-CLI cycle would refetch 14 years of
    data even though it's all already on Redis."""
    try:
        r = get_redis()
        _migrate_synced_index(r, "closed_positions",
                              "closed_positions:months:synced",
                              "closed_positions:synced_migrated")
        _migrate_synced_index(r, "deals",
                              "deals:months:synced",
                              "deals:synced_migrated")
        _migrate_deposits_withdrawals_per_month(r)
    except Exception as exc:
        print(f"[startup] Synced index migration failed: {exc}")


def _migrate_deposits_withdrawals_per_month(r: redis.Redis):
    """Walk deposits_withdrawals:tickets, group by month from each record's
    `time` field, populate deposits_withdrawals:month:{mk} SETs and
    deposits_withdrawals:months:synced. One-shot, marker-gated."""
    if r.get("deposits_withdrawals:migrated"):
        return
    tickets = list(r.smembers("deposits_withdrawals:tickets") or set())
    if not tickets:
        r.set("deposits_withdrawals:migrated", "1")
        return

    # Read all records in one pipeline pass.
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deposit_withdrawal:{t}")
    raw_records = pipe.execute()

    months: dict[str, list[str]] = {}
    for t, raw in zip(tickets, raw_records):
        if not raw:
            continue
        try:
            ts = int(json.loads(raw).get("time", 0) or 0)
        except Exception:
            continue
        if ts <= 0:
            continue
        mk = _month_key(ts)
        months.setdefault(mk, []).append(t)

    wpipe = r.pipeline()
    for mk, ts_list in months.items():
        for t in ts_list:
            wpipe.sadd(f"deposits_withdrawals:month:{mk}", t)
        wpipe.hset(f"deposits_withdrawals:month_meta:{mk}", mapping={
            "month": mk,
            "count": len(ts_list),
            "last_update": "",
            "synced": 1,
        })
        wpipe.sadd("deposits_withdrawals:months:synced", mk)
    wpipe.set("deposits_withdrawals:migrated", "1")
    wpipe.execute()
    print(f"[startup] D&W: backfilled per-month index across "
          f"{len(months)} months / {sum(len(v) for v in months.values())} records")


@app.on_event("startup")
def cleanup_legacy_balance_ops_keys():
    """One-shot purge of legacy `balance_op:*` / `balance_ops:*` Redis keys, replaced
    by the `deposits_withdrawals` feature. Marker-gated so the keyspace SCAN runs at
    most once per Redis instance."""
    try:
        r = get_redis()
        if r.get("balance_ops:purged"):
            return
        keys = _scan_keys(r, "balance_op:*") + _scan_keys(r, "balance_ops:*")
        if keys:
            _delete_keys(r, keys)
            print(f"[startup] Purged {len(keys)} legacy balance_op(s):* keys")
        r.set("balance_ops:purged", "1")
    except Exception as exc:
        print(f"[startup] Legacy balance_ops cleanup failed: {exc}")


def _month_meta_key(month_key: str) -> str:
    return f"closed_positions:month_meta:{month_key}"


def _deal_month_meta_key(month_key: str) -> str:
    return f"deals:month_meta:{month_key}"


def _dw_month_meta_key(month_key: str) -> str:
    return f"deposits_withdrawals:month_meta:{month_key}"


def _parse_date_param(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD")


def _month_key_from_date(value: date) -> str:
    return f"{value.year}-{value.month:02d}"


def _month_range(from_month: str, to_month: str) -> list[str]:
    start_year, start_month = [int(x) for x in from_month.split("-")]
    end_year, end_month = [int(x) for x in to_month.split("-")]
    if (start_year, start_month) > (end_year, end_month):
        raise HTTPException(status_code=400, detail="from_date must be before or equal to to_date")
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


# =============================================================================
# Pydantic models
# =============================================================================

class Position(BaseModel):
    ticket: int        # position ticket (= position_id)
    position_id: int   # same as ticket — explicit alias for joining with closed_positions
    login: int
    symbol: str
    cmd: int
    volume: float
    open_price: float
    price_current: float
    open_time: int
    time_update: int = 0   # last modification time (SL/TP change, etc.)
    pnl: float
    swap: float
    notional_value: float
    contract_size: float
    profit_currency: str
    computed_pnl: float = 0.0
    computed_swap: float = 0.0


class PositionsPayload(BaseModel):
    positions: list[Position]

    @field_validator("positions")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("positions array must not be empty")
        return v


class PositionsSnapshotBeginPayload(BaseModel):
    expected_chunks: int = 0
    expected_positions: int = 0

    @field_validator("expected_chunks", "expected_positions")
    @classmethod
    def must_not_be_negative(cls, v):
        if v < 0:
            raise ValueError("value must not be negative")
        return v


class PositionsSnapshotChunkPayload(BaseModel):
    chunk_index: int
    positions: list[Position]

    @field_validator("chunk_index")
    @classmethod
    def chunk_index_must_not_be_negative(cls, v):
        if v < 0:
            raise ValueError("chunk_index must not be negative")
        return v

    @field_validator("positions")
    @classmethod
    def positions_must_not_be_empty(cls, v):
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
    open_price: float = 0.0   # position's weighted average open price (deal.PricePosition)
    close_price: float        # this deal's execution price (deal.Price)
    volume_lots: float
    profit: float
    commission: float
    swap: float
    fee: float
    open_time: int = 0       # when position was originally opened (0 if unknown)
    close_time: int          # when position was closed
    contract_size: float = 0.0
    notional_value: float = 0.0  # volume_lots * contract_size * close_price
    comment: str = ""
    computed_profit: float = 0.0
    computed_commission: float = 0.0
    computed_swap: float = 0.0
    computed_fee: float = 0.0


class ClosedPositionsPayload(BaseModel):
    closed_positions: list[ClosedPosition]

    @field_validator("closed_positions")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("closed_positions array must not be empty")
        return v


class MonthlyClosedPositionsPayload(BaseModel):
    closed_positions: list[ClosedPosition] = []


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
    comment: str = ""        # raw MT5 Comment field
    crm_id: str = ""         # alias of `comment`, kept for back-compat
    currency: str = ""       # account deposit currency (resolved from the account's group)
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
    external_id: str = ""
    order: int
    position_id: int
    login: int
    dealer: int = 0
    symbol: str
    action: int
    entry: int
    digits: int = 0
    digits_currency: int = 0
    contract_size: float = 0.0
    price: float
    price_sl: float
    price_tp: float
    price_position: float = 0.0
    price_gateway: float = 0.0
    market_bid: float = 0.0
    market_ask: float = 0.0
    market_last: float = 0.0
    volume_lots: float
    volume_closed_lots: float
    volume_ext: int = 0
    volume_closed_ext: int = 0
    profit: float
    profit_raw: float = 0.0
    commission: float
    swap: float
    fee: float
    rate_profit: float = 0.0
    rate_margin: float = 0.0
    tick_value: float = 0.0
    tick_size: float = 0.0
    reason: int
    flags: int = 0
    modification_flags: int = 0
    expert_id: int = 0
    gateway: str = ""
    time: int
    time_msc: int
    comment: str = ""
    computed_profit: float = 0.0
    computed_commission: float = 0.0
    computed_swap: float = 0.0
    computed_fee: float = 0.0


class DealsPayload(BaseModel):
    deals: list[Deal]

    @field_validator("deals")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("deals array must not be empty")
        return v


class DealMonthSnapshotBeginPayload(BaseModel):
    expected_chunks: int = 0
    expected_deals: int = 0

    @field_validator("expected_chunks", "expected_deals")
    @classmethod
    def must_not_be_negative(cls, v):
        if v < 0:
            raise ValueError("value must not be negative")
        return v


class DealMonthSnapshotChunkPayload(BaseModel):
    chunk_index: int
    deals: list[Deal]

    @field_validator("chunk_index")
    @classmethod
    def chunk_index_must_not_be_negative(cls, v):
        if v < 0:
            raise ValueError("chunk_index must not be negative")
        return v

    @field_validator("deals")
    @classmethod
    def deals_must_not_be_empty(cls, v):
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


class DepositWithdrawal(BaseModel):
    ticket:              int
    login:               int
    time:                int
    time_msc:            int = 0
    amount:              float           # deal.Profit() — positive=deposit, negative=withdrawal
    comment:             str = ""        # "Deposit, Wire Transfer" / "Withdrawal,Wire Transfer" / ...
    currency:            str = ""        # account deposit currency
    computed_amount_usd: float = 0.0
    direction:           str = ""        # "deposit" or "withdrawal"


class DepositsWithdrawalsPayload(BaseModel):
    deposits_withdrawals: list[DepositWithdrawal]


class HistoricalRate(BaseModel):
    symbol:     str
    date:       str       # YYYY-MM-DD (UTC)
    open_bid:   float = 0.0
    open_ask:   float = 0.0
    time_open:  int   = 0
    close_bid:  float = 0.0
    close_ask:  float = 0.0
    time_close: int   = 0


class HistoricalRatesPayload(BaseModel):
    rates: list[HistoricalRate]


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
    pipe.scard("deposits_withdrawals:tickets")
    pipe.scard("deposits_withdrawals:months:synced")
    pipe.scard("historical_rates:symbols")
    pipe.scard("historical_rates:years_synced")
    pipe.get("positions:last_update")
    pipe.get("accounts:last_update")
    pipe.get("deals:last_update")
    pipe.get("closed_positions:last_update")
    pipe.get("rates:last_update")
    pipe.get("deposits_withdrawals:last_update")
    pipe.get("historical_rates:last_update")
    (pos, acct, deal, cpx, rate, dwc, dw_months, hr_syms, hr_years,
     pts, ats, dts, cpts, rts, dwts, hrts) = pipe.execute()

    # Total historical-rate record count = sum of per-symbol date sets.
    # Cheap because the symbols set is small (~18 entries).
    hr_count = 0
    sym_set = r.smembers("historical_rates:symbols") or set()
    if sym_set:
        cpipe = r.pipeline()
        for s in sym_set:
            cpipe.scard(f"historical_rates:{s}:dates")
        hr_count = sum(cpipe.execute())

    return {
        "positions":             {"count": pos,  "last_update": pts},
        "accounts":              {"count": acct, "last_update": ats},
        "deals":                 {"count": deal, "last_update": dts},
        "closed_positions":      {"count": cpx,  "last_update": cpts},
        "rates":                 {"count": rate, "last_update": rts},
        "deposits_withdrawals":  {"count": dwc,  "months_synced": dw_months,
                                  "last_update": dwts},
        "historical_rates":      {"count": hr_count, "symbols": hr_syms,
                                  "years_synced": hr_years, "last_update": hrts},
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
    pipe.delete("positions:watermark")
    pipe.set("positions:latest:raw", payload.model_dump_json())
    pipe.set("positions:last_update", now)
    max_ts = 0
    for pos in payload.positions:
        pipe.set(f"position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.sadd("positions:tickets", str(pos.ticket))
        if pos.time_update > max_ts:
            max_ts = pos.time_update
    if max_ts:
        pipe.set("positions:watermark", str(max_ts))
    pipe.execute()

    return {"success": True, "positions_processed": len(payload.positions),
            "tickets": [p.ticket for p in payload.positions], "stale_removed": len(stale)}


SNAPSHOT_TTL_SECONDS = 24 * 60 * 60


def _snapshot_prefix(snapshot_id: str) -> str:
    return f"positions:snapshot:{snapshot_id}"


@app.post("/positions/snapshot/begin", dependencies=[Security(verify_token)])
def begin_positions_snapshot(payload: PositionsSnapshotBeginPayload):
    r = get_redis()
    snapshot_id = uuid.uuid4().hex
    prefix = _snapshot_prefix(snapshot_id)
    now = datetime.now(timezone.utc).isoformat()
    r.hset(f"{prefix}:meta", mapping={
        "created_at": now,
        "expected_chunks": payload.expected_chunks,
        "expected_positions": payload.expected_positions,
        "max_time_update": 0,
    })
    r.expire(f"{prefix}:meta", SNAPSHOT_TTL_SECONDS)
    return {"success": True, "snapshot_id": snapshot_id}


@app.post("/positions/snapshot/{snapshot_id}/chunk", dependencies=[Security(verify_token)])
def post_positions_snapshot_chunk(snapshot_id: str, payload: PositionsSnapshotChunkPayload):
    r = get_redis()
    prefix = _snapshot_prefix(snapshot_id)
    meta_key = f"{prefix}:meta"
    meta = r.hgetall(meta_key)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")

    expected_chunks = int(meta.get("expected_chunks") or 0)
    if expected_chunks and payload.chunk_index >= expected_chunks:
        raise HTTPException(status_code=400, detail="chunk_index exceeds expected_chunks")

    max_ts = int(meta.get("max_time_update") or 0)
    tickets_key = f"{prefix}:tickets"
    chunks_key = f"{prefix}:chunks"
    pipe = r.pipeline()
    for pos in payload.positions:
        pipe.set(f"{prefix}:position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.expire(f"{prefix}:position:{pos.ticket}", SNAPSHOT_TTL_SECONDS)
        pipe.sadd(tickets_key, str(pos.ticket))
        if pos.time_update > max_ts:
            max_ts = pos.time_update
    pipe.sadd(chunks_key, str(payload.chunk_index))
    pipe.hset(meta_key, "max_time_update", max_ts)
    pipe.expire(tickets_key, SNAPSHOT_TTL_SECONDS)
    pipe.expire(chunks_key, SNAPSHOT_TTL_SECONDS)
    pipe.expire(meta_key, SNAPSHOT_TTL_SECONDS)
    pipe.execute()

    return {"success": True, "snapshot_id": snapshot_id,
            "chunk_index": payload.chunk_index, "positions_processed": len(payload.positions)}


@app.post("/positions/snapshot/{snapshot_id}/commit", dependencies=[Security(verify_token)])
def commit_positions_snapshot(snapshot_id: str):
    r = get_redis()
    prefix = _snapshot_prefix(snapshot_id)
    meta_key = f"{prefix}:meta"
    meta = r.hgetall(meta_key)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")

    expected_chunks = int(meta.get("expected_chunks") or 0)
    expected_positions = int(meta.get("expected_positions") or 0)
    chunks_seen = r.scard(f"{prefix}:chunks")
    if expected_chunks and chunks_seen != expected_chunks:
        raise HTTPException(status_code=409,
                            detail=f"Snapshot has {chunks_seen}/{expected_chunks} chunks")

    new_tickets = r.smembers(f"{prefix}:tickets") or set()
    if expected_positions and len(new_tickets) != expected_positions:
        raise HTTPException(status_code=409,
                            detail=f"Snapshot has {len(new_tickets)}/{expected_positions} positions")

    pipe = r.pipeline()
    for t in new_tickets:
        pipe.get(f"{prefix}:position:{t}")
    raw_records = pipe.execute()
    records = [(t, d) for t, d in zip(new_tickets, raw_records) if d]
    if len(records) != len(new_tickets):
        raise HTTPException(status_code=409, detail="Snapshot is missing one or more position records")

    old_tickets = r.smembers("positions:tickets") or set()
    stale = old_tickets - new_tickets
    now = datetime.now(timezone.utc).isoformat()
    max_ts = int(meta.get("max_time_update") or 0)

    pipe = r.pipeline()
    for t in stale:
        pipe.delete(f"position:{t}")
    pipe.delete("positions:tickets")
    pipe.delete("positions:watermark")
    pipe.set("positions:last_update", now)
    pipe.set("positions:latest:raw", json.dumps({
        "snapshot_id": snapshot_id,
        "positions_count": len(records),
        "committed_at": now,
    }))
    for t, data in records:
        pipe.set(f"position:{t}", data)
        pipe.sadd("positions:tickets", t)
    if max_ts:
        pipe.set("positions:watermark", str(max_ts))
    pipe.execute()

    _delete_keys(r, _scan_keys(r, f"{prefix}:*"))

    return {"success": True, "snapshot_id": snapshot_id,
            "positions_processed": len(records), "stale_removed": len(stale)}


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


@app.get("/positions/watermark", dependencies=[Security(verify_token)])
def positions_watermark():
    r = get_redis()
    wm = r.get("positions:watermark")
    count = r.scard("positions:tickets")
    return {"max_time_update": int(wm) if wm else 0, "count": count or 0}


@app.post("/positions/upsert", dependencies=[Security(verify_token)])
def upsert_positions(payload: PositionsPayload):
    """Upsert positions without deleting stale records. Updates watermark."""
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    wm_raw = r.get("positions:watermark")
    current_wm = int(wm_raw) if wm_raw else 0
    max_ts = current_wm

    pipe = r.pipeline()
    pipe.set("positions:last_update", now)
    for pos in payload.positions:
        pipe.set(f"position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.sadd("positions:tickets", str(pos.ticket))
        if pos.time_update > max_ts:
            max_ts = pos.time_update
    if max_ts > current_wm:
        pipe.set("positions:watermark", str(max_ts))
    pipe.execute()

    return {"success": True, "positions_processed": len(payload.positions),
            "tickets": [p.ticket for p in payload.positions]}


# =============================================================================
# Closed Positions (YTD closing deals)
# =============================================================================

@app.post("/closed_positions/reset", dependencies=[Security(verify_token)])
def reset_closed_positions():
    r = get_redis()
    tickets = r.smembers("closed_positions:tickets") or set()
    month_keys = _scan_keys(r, "closed_positions:month:*")
    month_meta_keys = _scan_keys(r, "closed_positions:month_meta:*")
    pipe = r.pipeline()
    for t in tickets:
        pipe.delete(f"closed_position:{t}")
    for k in month_keys:
        pipe.delete(k)
    for k in month_meta_keys:
        pipe.delete(k)
    pipe.delete("closed_positions:months:synced")
    pipe.delete("closed_positions:tickets")
    pipe.delete("closed_positions:last_update")
    pipe.delete("closed_positions:watermark")
    pipe.execute()
    return {"success": True, "cleared": len(tickets)}


def _month_key(ts: int) -> str:
    from datetime import datetime, timezone as _tz
    d = datetime.fromtimestamp(ts, tz=_tz.utc)
    return f"{d.year}-{d.month:02d}"


def _mark_closed_positions_month(r: redis.Redis, month_key: str, count: int, now: str):
    r.hset(_month_meta_key(month_key), mapping={
        "month": month_key,
        "count": count,
        "last_update": now,
        "synced": 1,
    })
    r.sadd("closed_positions:months:synced", month_key)


@app.post("/closed_positions", dependencies=[Security(verify_token)])
def post_closed_positions(payload: ClosedPositionsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    months_touched: set[str] = set()
    pipe = r.pipeline()
    pipe.set("closed_positions:last_update", now)
    for cp in payload.closed_positions:
        mk = _month_key(cp.close_time)
        pipe.set(f"closed_position:{cp.ticket}", json.dumps(cp.model_dump()))
        pipe.sadd("closed_positions:tickets", str(cp.ticket))
        pipe.sadd(f"closed_positions:month:{mk}", str(cp.ticket))
        months_touched.add(mk)
    if months_touched:
        pipe.sadd("closed_positions:months:synced", *months_touched)
    pipe.execute()
    return {"success": True, "closed_positions_processed": len(payload.closed_positions),
            "tickets": [cp.ticket for cp in payload.closed_positions]}


@app.get("/closed_positions/months", dependencies=[Security(verify_token)])
def closed_positions_months():
    """Return month counts plus explicit sync metadata."""
    r = get_redis()
    synced = sorted(r.smembers("closed_positions:months:synced") or set())
    if not synced:
        return {"months": {}, "metadata": {}}
    pipe = r.pipeline()
    for mk in synced:
        pipe.hgetall(_month_meta_key(mk))
        pipe.scard(f"closed_positions:month:{mk}")
    out = pipe.execute()
    months, metadata = {}, {}
    for i, mk in enumerate(synced):
        meta = out[i * 2] or {}
        set_count = out[i * 2 + 1] or 0
        count = int(meta.get("count") or set_count)
        months[mk] = count
        metadata[mk] = {
            "month": mk,
            "count": count,
            "last_update": meta.get("last_update"),
            "synced": str(meta.get("synced", "1")) == "1",
        }
    return {"months": months, "metadata": metadata}


@app.get("/closed_positions/months/check", dependencies=[Security(verify_token)])
def check_closed_positions_months(from_date: str, to_date: str):
    r = get_redis()
    from_month = _month_key_from_date(_parse_date_param(from_date, "from_date"))
    to_month = _month_key_from_date(_parse_date_param(to_date, "to_date"))
    required_months = _month_range(from_month, to_month)
    present = set(r.smembers("closed_positions:months:synced") or set())
    present_months = [m for m in required_months if m in present]
    missing_months = [m for m in required_months if m not in present]
    current_month = _month_key_from_date(datetime.now(timezone.utc).date())
    return {
        "ok": not missing_months,
        "from_month": from_month,
        "to_month": to_month,
        "missing_months": missing_months,
        "present_months": present_months,
        "current_month": current_month,
    }


@app.get("/closed_positions/month/{year}/{month}", dependencies=[Security(verify_token)])
def get_closed_positions_month(year: int, month: int):
    """Return every closed position whose `close_time` falls in {year}-{month},
    sorted by close_time ascending. Reads only the per-month SET — no SCAN."""
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    mk = f"{year}-{month:02d}"
    r = get_redis()
    tickets = list(r.smembers(f"closed_positions:month:{mk}") or set())
    if not tickets:
        return {"month": mk, "count": 0, "closed_positions": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"closed_position:{t}")
    rows = sorted([json.loads(d) for d in pipe.execute() if d],
                  key=lambda x: x.get("close_time", 0))
    return {"month": mk, "count": len(rows), "closed_positions": rows}


@app.post("/closed_positions/month/{year}/{month}", dependencies=[Security(verify_token)])
def post_closed_positions_month(year: int, month: int, payload: MonthlyClosedPositionsPayload):
    """Snapshot-replace closed positions for a single calendar month."""
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    mk = f"{year}-{month:02d}"
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    # Remove old records for this month
    old_tickets = r.smembers(f"closed_positions:month:{mk}") or set()
    pipe = r.pipeline()
    for t in old_tickets:
        pipe.delete(f"closed_position:{t}")
        pipe.srem("closed_positions:tickets", t)
    pipe.delete(f"closed_positions:month:{mk}")
    pipe.execute()

    # Insert new batch
    pipe = r.pipeline()
    pipe.set("closed_positions:last_update", now)
    pipe.hset(_month_meta_key(mk), mapping={
        "month": mk,
        "count": len(payload.closed_positions),
        "last_update": now,
        "synced": 1,
    })
    pipe.sadd("closed_positions:months:synced", mk)
    for cp in payload.closed_positions:
        pipe.set(f"closed_position:{cp.ticket}", json.dumps(cp.model_dump()))
        pipe.sadd("closed_positions:tickets", str(cp.ticket))
        pipe.sadd(f"closed_positions:month:{mk}", str(cp.ticket))
    pipe.execute()

    return {"success": True, "month": mk,
            "closed_positions_processed": len(payload.closed_positions),
            "replaced": len(old_tickets)}


@app.get("/closed_positions", dependencies=[Security(verify_token)])
def get_closed_positions():
    r = get_redis()
    tickets = list(r.smembers("closed_positions:tickets"))
    if not tickets:
        return {"count": 0, "closed_positions": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"closed_position:{t}")
    records = sorted([json.loads(d) for d in pipe.execute() if d],
                     key=lambda x: x.get("close_time", 0), reverse=True)
    return {"count": len(records), "closed_positions": records}


@app.get("/closed_positions/{ticket}", dependencies=[Security(verify_token)])
def get_closed_position(ticket: int):
    r = get_redis()
    data = r.get(f"closed_position:{ticket}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Closed position {ticket} not found")
    return json.loads(data)


@app.get("/closed_positions/watermark", dependencies=[Security(verify_token)])
def closed_positions_watermark():
    r = get_redis()
    wm = r.get("closed_positions:watermark")
    count = r.scard("closed_positions:tickets")
    return {"max_close_time": int(wm) if wm else 0, "count": count or 0}


@app.post("/closed_positions/upsert", dependencies=[Security(verify_token)])
def upsert_closed_positions(payload: ClosedPositionsPayload):
    """Upsert closed positions without resetting. Updates close_time watermark
    and writes the per-month index, but intentionally does NOT mark months as
    `synced`. The rolling-window CLI step calls this every cycle for the
    current month — flipping synced here would short-circuit the
    historical-missing snapshot-replace flow at month rollover."""
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    wm_raw = r.get("closed_positions:watermark")
    current_wm = int(wm_raw) if wm_raw else 0
    max_ts = current_wm

    pipe = r.pipeline()
    pipe.set("closed_positions:last_update", now)
    for cp in payload.closed_positions:
        mk = _month_key(cp.close_time)
        pipe.set(f"closed_position:{cp.ticket}", json.dumps(cp.model_dump()))
        pipe.sadd("closed_positions:tickets", str(cp.ticket))
        pipe.sadd(f"closed_positions:month:{mk}", str(cp.ticket))
        if cp.close_time > max_ts:
            max_ts = cp.close_time
    if max_ts > current_wm:
        pipe.set("closed_positions:watermark", str(max_ts))
    pipe.execute()

    return {"success": True, "closed_positions_processed": len(payload.closed_positions),
            "tickets": [cp.ticket for cp in payload.closed_positions]}


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
    return {"success": True, "accounts_processed": len(payload.accounts)}


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

def _validate_month(year: int, month: int) -> str:
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    return f"{year}-{month:02d}"


def _deal_snapshot_prefix(month_key: str, snapshot_id: str) -> str:
    return f"deals:month_snapshot:{month_key}:{snapshot_id}"


@app.post("/deals", dependencies=[Security(verify_token)])
def post_deals(payload: DealsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    months_touched: set[str] = set()
    pipe = r.pipeline()
    pipe.set("deals:last_update", now)
    for deal in payload.deals:
        mk = _month_key(deal.time)
        pipe.set(f"deal:{deal.ticket}", json.dumps(deal.model_dump()))
        pipe.sadd("deals:tickets", str(deal.ticket))
        pipe.sadd(f"deals:month:{mk}", str(deal.ticket))
        months_touched.add(mk)
    if months_touched:
        pipe.sadd("deals:months:synced", *months_touched)
    pipe.execute()
    return {"success": True, "deals_processed": len(payload.deals)}


@app.get("/deals/months", dependencies=[Security(verify_token)])
def deal_months():
    r = get_redis()
    synced = sorted(r.smembers("deals:months:synced") or set())
    if not synced:
        return {"months": {}, "metadata": {}}
    pipe = r.pipeline()
    for mk in synced:
        pipe.hgetall(_deal_month_meta_key(mk))
        pipe.scard(f"deals:month:{mk}")
    out = pipe.execute()
    months, metadata = {}, {}
    for i, mk in enumerate(synced):
        meta = out[i * 2] or {}
        set_count = out[i * 2 + 1] or 0
        count = int(meta.get("count") or set_count)
        months[mk] = count
        metadata[mk] = {
            "month": mk,
            "count": count,
            "last_update": meta.get("last_update"),
            "synced": str(meta.get("synced", "1")) == "1",
        }
    return {"months": months, "metadata": metadata}


@app.get("/deals/months/check", dependencies=[Security(verify_token)])
def check_deal_months(from_date: str, to_date: str):
    r = get_redis()
    from_month = _month_key_from_date(_parse_date_param(from_date, "from_date"))
    to_month = _month_key_from_date(_parse_date_param(to_date, "to_date"))
    required_months = _month_range(from_month, to_month)
    present = set(r.smembers("deals:months:synced") or set())
    present_months = [m for m in required_months if m in present]
    missing_months = [m for m in required_months if m not in present]
    current_month = _month_key_from_date(datetime.now(timezone.utc).date())
    return {
        "ok": not missing_months,
        "from_month": from_month,
        "to_month": to_month,
        "missing_months": missing_months,
        "present_months": present_months,
        "current_month": current_month,
    }


@app.get("/deals/month/{year}/{month}", dependencies=[Security(verify_token)])
def get_deals_month(year: int, month: int):
    """Return every deal whose `time` falls in {year}-{month}, sorted by time
    ascending. Reads only the per-month SET — no SCAN."""
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    mk = f"{year}-{month:02d}"
    r = get_redis()
    tickets = list(r.smembers(f"deals:month:{mk}") or set())
    if not tickets:
        return {"month": mk, "count": 0, "deals": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deal:{t}")
    rows = sorted([json.loads(d) for d in pipe.execute() if d],
                  key=lambda x: x.get("time", 0))
    return {"month": mk, "count": len(rows), "deals": rows}


@app.post("/deals/month/{year}/{month}/snapshot/begin", dependencies=[Security(verify_token)])
def begin_deal_month_snapshot(year: int, month: int, payload: DealMonthSnapshotBeginPayload):
    mk = _validate_month(year, month)
    r = get_redis()
    snapshot_id = uuid.uuid4().hex
    prefix = _deal_snapshot_prefix(mk, snapshot_id)
    r.hset(f"{prefix}:meta", mapping={
        "month": mk,
        "expected_chunks": payload.expected_chunks,
        "expected_deals": payload.expected_deals,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    r.expire(f"{prefix}:meta", 86400)
    r.expire(f"{prefix}:chunks", 86400)
    return {"success": True, "snapshot_id": snapshot_id, "month": mk}


@app.post("/deals/month/{year}/{month}/snapshot/{snapshot_id}/chunk", dependencies=[Security(verify_token)])
def post_deal_month_snapshot_chunk(
    year: int,
    month: int,
    snapshot_id: str,
    payload: DealMonthSnapshotChunkPayload,
):
    mk = _validate_month(year, month)
    r = get_redis()
    prefix = _deal_snapshot_prefix(mk, snapshot_id)
    if not r.exists(f"{prefix}:meta"):
        raise HTTPException(status_code=404, detail="Snapshot not found")

    chunk_key = f"{prefix}:chunk:{payload.chunk_index}"
    r.set(chunk_key, json.dumps([d.model_dump() for d in payload.deals]), ex=86400)
    r.sadd(f"{prefix}:chunks", str(payload.chunk_index))
    r.expire(f"{prefix}:chunks", 86400)
    return {"success": True, "month": mk, "chunk_index": payload.chunk_index,
            "deals_processed": len(payload.deals)}


@app.post("/deals/month/{year}/{month}/snapshot/{snapshot_id}/commit", dependencies=[Security(verify_token)])
def commit_deal_month_snapshot(year: int, month: int, snapshot_id: str):
    mk = _validate_month(year, month)
    r = get_redis()
    prefix = _deal_snapshot_prefix(mk, snapshot_id)
    meta_key = f"{prefix}:meta"
    meta = r.hgetall(meta_key)
    if not meta:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    expected_chunks = int(meta.get("expected_chunks") or 0)
    expected_deals = int(meta.get("expected_deals") or 0)
    received = {int(x) for x in (r.smembers(f"{prefix}:chunks") or set())}
    missing = [i for i in range(expected_chunks) if i not in received]
    if missing:
        raise HTTPException(status_code=409, detail={"missing_chunks": missing})

    deals: list[dict] = []
    chunk_keys = [f"{prefix}:chunk:{i}" for i in range(expected_chunks)]
    if chunk_keys:
        raw_chunks = r.mget(chunk_keys)
        for i, raw in enumerate(raw_chunks):
            if raw is None:
                raise HTTPException(status_code=409, detail={"missing_chunks": [i]})
            deals.extend(json.loads(raw))

    if len(deals) != expected_deals:
        raise HTTPException(
            status_code=409,
            detail={"expected_deals": expected_deals, "received_deals": len(deals)},
        )

    old_tickets = r.smembers(f"deals:month:{mk}") or set()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    for ticket in old_tickets:
        pipe.delete(f"deal:{ticket}")
        pipe.srem("deals:tickets", ticket)
    pipe.delete(f"deals:month:{mk}")
    pipe.execute()

    pipe = r.pipeline()
    pipe.set("deals:last_update", now)
    pipe.hset(_deal_month_meta_key(mk), mapping={
        "month": mk,
        "count": len(deals),
        "last_update": now,
        "synced": 1,
    })
    pipe.sadd("deals:months:synced", mk)
    for deal in deals:
        ticket = str(deal["ticket"])
        pipe.set(f"deal:{ticket}", json.dumps(deal))
        pipe.sadd("deals:tickets", ticket)
        pipe.sadd(f"deals:month:{mk}", ticket)
    pipe.execute()

    cleanup = chunk_keys + [meta_key, f"{prefix}:chunks"]
    _delete_keys(r, cleanup)
    return {"success": True, "month": mk, "deals_processed": len(deals),
            "replaced": len(old_tickets)}


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


# =============================================================================
# Deposits & Withdrawals (DEAL_BALANCE only — action=2)
# =============================================================================

@app.post("/deposits_withdrawals/reset", dependencies=[Security(verify_token)])
def reset_deposits_withdrawals():
    r = get_redis()
    tickets = r.smembers("deposits_withdrawals:tickets") or set()
    pipe = r.pipeline()
    for t in tickets:
        pipe.delete(f"deposit_withdrawal:{t}")
    pipe.delete("deposits_withdrawals:tickets")
    pipe.delete("deposits_withdrawals:last_update")
    pipe.execute()
    return {"success": True, "cleared": len(tickets)}


@app.post("/deposits_withdrawals", dependencies=[Security(verify_token)])
def post_deposits_withdrawals(payload: DepositsWithdrawalsPayload):
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    for d in payload.deposits_withdrawals:
        pipe.set(f"deposit_withdrawal:{d.ticket}", d.model_dump_json())
        pipe.sadd("deposits_withdrawals:tickets", str(d.ticket))
    pipe.set("deposits_withdrawals:last_update", now)
    pipe.execute()
    return {"success": True, "deposits_withdrawals_processed": len(payload.deposits_withdrawals)}


@app.get("/deposits_withdrawals", dependencies=[Security(verify_token)])
def get_all_deposits_withdrawals():
    r = get_redis()
    tickets = list(r.smembers("deposits_withdrawals:tickets"))
    if not tickets:
        return {"count": 0, "deposits_withdrawals": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deposit_withdrawal:{t}")
    rows = sorted([json.loads(d) for d in pipe.execute() if d],
                  key=lambda x: x["time"], reverse=True)
    return {"count": len(rows), "deposits_withdrawals": rows}


@app.get("/deposits_withdrawals/months/check", dependencies=[Security(verify_token)])
def check_deposits_withdrawals_months(from_date: str, to_date: str):
    """Tells the CLI which calendar months in [from_date, to_date] are not
    yet on Redis. Single SMEMBERS read against deposits_withdrawals:months:synced —
    no SCAN."""
    r = get_redis()
    from_month = _month_key_from_date(_parse_date_param(from_date, "from_date"))
    to_month = _month_key_from_date(_parse_date_param(to_date, "to_date"))
    required_months = _month_range(from_month, to_month)
    present = set(r.smembers("deposits_withdrawals:months:synced") or set())
    present_months = [m for m in required_months if m in present]
    missing_months = [m for m in required_months if m not in present]
    current_month = _month_key_from_date(datetime.now(timezone.utc).date())
    return {
        "ok": not missing_months,
        "from_month": from_month,
        "to_month": to_month,
        "missing_months": missing_months,
        "present_months": present_months,
        "current_month": current_month,
    }


@app.get("/deposits_withdrawals/months/synced", dependencies=[Security(verify_token)])
def deposits_withdrawals_months_synced():
    r = get_redis()
    return {"months": sorted(r.smembers("deposits_withdrawals:months:synced") or set())}


@app.get("/deposits_withdrawals/months", dependencies=[Security(verify_token)])
def deposits_withdrawals_months():
    """Per-month count + last_update meta. Synced-only, pipelined — no SCAN."""
    r = get_redis()
    synced = sorted(r.smembers("deposits_withdrawals:months:synced") or set())
    if not synced:
        return {"months": {}, "metadata": {}}
    pipe = r.pipeline()
    for mk in synced:
        pipe.hgetall(_dw_month_meta_key(mk))
        pipe.scard(f"deposits_withdrawals:month:{mk}")
    out = pipe.execute()
    months, metadata = {}, {}
    for i, mk in enumerate(synced):
        meta = out[i * 2] or {}
        set_count = out[i * 2 + 1] or 0
        count = int(meta.get("count") or set_count)
        months[mk] = count
        metadata[mk] = {
            "month": mk,
            "count": count,
            "last_update": meta.get("last_update") or None,
            "synced": str(meta.get("synced", "1")) == "1",
        }
    return {"months": months, "metadata": metadata}


@app.post("/deposits_withdrawals/upsert", dependencies=[Security(verify_token)])
def upsert_deposits_withdrawals(payload: DepositsWithdrawalsPayload):
    """Upsert deposits/withdrawals without snapshot-replacing the month.
    Used by the rolling-window step on each CLI cycle. Like
    upsert_closed_positions, it populates the per-month index but does NOT
    mark months as `synced` — that flip is reserved for the explicit
    /month/{Y}/{M} snapshot-replace path so historical-missing flow keeps
    working at month rollover."""
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    pipe.set("deposits_withdrawals:last_update", now)
    for d in payload.deposits_withdrawals:
        mk = _month_key(d.time)
        pipe.set(f"deposit_withdrawal:{d.ticket}", d.model_dump_json())
        pipe.sadd("deposits_withdrawals:tickets", str(d.ticket))
        pipe.sadd(f"deposits_withdrawals:month:{mk}", str(d.ticket))
    pipe.execute()
    return {"success": True,
            "deposits_withdrawals_processed": len(payload.deposits_withdrawals)}


@app.post("/deposits_withdrawals/month/{year}/{month}", dependencies=[Security(verify_token)])
def post_deposits_withdrawals_month(year: int, month: int, payload: DepositsWithdrawalsPayload):
    """Snapshot-replace deposits & withdrawals for a single calendar month.
    Mirrors post_closed_positions_month: drop existing rows for the month,
    insert the new batch, mark synced + meta, bump last_update."""
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    mk = f"{year}-{month:02d}"
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()

    # Drop old per-month rows.
    old_tickets = r.smembers(f"deposits_withdrawals:month:{mk}") or set()
    pipe = r.pipeline()
    for t in old_tickets:
        pipe.delete(f"deposit_withdrawal:{t}")
        pipe.srem("deposits_withdrawals:tickets", t)
    pipe.delete(f"deposits_withdrawals:month:{mk}")
    pipe.execute()

    # Insert new batch.
    pipe = r.pipeline()
    pipe.set("deposits_withdrawals:last_update", now)
    pipe.hset(_dw_month_meta_key(mk), mapping={
        "month": mk,
        "count": len(payload.deposits_withdrawals),
        "last_update": now,
        "synced": 1,
    })
    pipe.sadd("deposits_withdrawals:months:synced", mk)
    for d in payload.deposits_withdrawals:
        pipe.set(f"deposit_withdrawal:{d.ticket}", d.model_dump_json())
        pipe.sadd("deposits_withdrawals:tickets", str(d.ticket))
        pipe.sadd(f"deposits_withdrawals:month:{mk}", str(d.ticket))
    pipe.execute()

    return {"success": True, "month": mk,
            "deposits_withdrawals_processed": len(payload.deposits_withdrawals),
            "replaced": len(old_tickets)}


@app.get("/deposits_withdrawals/month/{year}/{month}", dependencies=[Security(verify_token)])
def get_deposits_withdrawals_month(year: int, month: int):
    """Return every deposit / withdrawal whose `time` falls in {year}-{month}, sorted
    by time ascending. Reads only the per-month SET — no SCAN, no global filter."""
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="month must be 1..12")
    mk = f"{year}-{month:02d}"
    r = get_redis()
    tickets = list(r.smembers(f"deposits_withdrawals:month:{mk}") or set())
    if not tickets:
        return {"month": mk, "count": 0, "deposits_withdrawals": []}
    pipe = r.pipeline()
    for t in tickets:
        pipe.get(f"deposit_withdrawal:{t}")
    rows = sorted([json.loads(d) for d in pipe.execute() if d],
                  key=lambda x: x.get("time", 0))
    return {"month": mk, "count": len(rows), "deposits_withdrawals": rows}


@app.get("/deposits_withdrawals/{ticket}", dependencies=[Security(verify_token)])
def get_deposit_withdrawal(ticket: int):
    r = get_redis()
    data = r.get(f"deposit_withdrawal:{ticket}")
    if data is None:
        raise HTTPException(status_code=404, detail=f"Deposit/withdrawal {ticket} not found")
    return json.loads(data)


# =============================================================================
# Historical Rates (daily close per symbol, indexed by symbol + YYYY-MM-DD)
# =============================================================================

@app.get("/historical_rates/years/synced", dependencies=[Security(verify_token)])
def historical_rates_years_synced():
    """Return the set of fully-backfilled calendar years. MT5-Elise reads
    this to skip years it's already pushed."""
    r = get_redis()
    years = sorted(int(y) for y in (r.smembers("historical_rates:years_synced") or set()))
    return {"years": years}


@app.get("/historical_rates/symbols", dependencies=[Security(verify_token)])
def historical_rates_symbols():
    r = get_redis()
    return {"symbols": sorted(r.smembers("historical_rates:symbols") or set())}


@app.get("/historical_rates/{symbol}/dates", dependencies=[Security(verify_token)])
def historical_rates_dates(symbol: str):
    """Sorted list of YYYY-MM-DD dates this symbol has data for. Used by the
    MT5-Elise backfill job to skip (symbol, year) combos already present."""
    r = get_redis()
    dates = sorted(r.smembers(f"historical_rates:{symbol}:dates") or set())
    return {"symbol": symbol, "count": len(dates), "dates": dates}


@app.post("/historical_rates/year/{year}", dependencies=[Security(verify_token)])
def post_historical_rates_year(year: int, payload: HistoricalRatesPayload):
    """Snapshot-replace a full calendar year of daily-close rates across all
    symbols in the payload. Marks the year as synced so subsequent backfill
    ticks skip it."""
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    year_prefix = f"{year}-"

    # Sweep every existing symbol's date set, drop any date in `year`.
    pipe = r.pipeline()
    for sym in (r.smembers("historical_rates:symbols") or set()):
        existing = r.smembers(f"historical_rates:{sym}:dates") or set()
        for d in existing:
            if d.startswith(year_prefix):
                pipe.delete(f"historical_rate:{sym}:{d}")
                pipe.srem(f"historical_rates:{sym}:dates", d)
    # Insert new payload
    for hr in payload.rates:
        pipe.set(f"historical_rate:{hr.symbol}:{hr.date}", json.dumps(hr.model_dump()))
        pipe.sadd(f"historical_rates:{hr.symbol}:dates", hr.date)
        pipe.sadd("historical_rates:symbols", hr.symbol)
    pipe.sadd("historical_rates:years_synced", str(year))
    pipe.set("historical_rates:last_update", now)
    pipe.execute()
    return {"success": True, "year": year, "rates_processed": len(payload.rates)}


@app.post("/historical_rates/year/{year}/mark_synced", dependencies=[Security(verify_token)])
def mark_historical_rates_year_synced(year: int):
    """Flag a calendar year as fully backfilled. MT5-Elise calls this once
    every requested symbol for the year either has data on file or returned
    zero bars from MT5, so the year doesn't get re-attempted."""
    r = get_redis()
    r.sadd("historical_rates:years_synced", str(year))
    r.set("historical_rates:last_update", datetime.now(timezone.utc).isoformat())
    return {"success": True, "year": year}


@app.post("/historical_rates/upsert", dependencies=[Security(verify_token)])
def upsert_historical_rates(payload: HistoricalRatesPayload):
    """Idempotent upsert — used by the daily forward job. No year-replace,
    no marker change. Same record overwrites itself if pushed twice."""
    r = get_redis()
    now = datetime.now(timezone.utc).isoformat()
    pipe = r.pipeline()
    for hr in payload.rates:
        pipe.set(f"historical_rate:{hr.symbol}:{hr.date}", json.dumps(hr.model_dump()))
        pipe.sadd(f"historical_rates:{hr.symbol}:dates", hr.date)
        pipe.sadd("historical_rates:symbols", hr.symbol)
    pipe.set("historical_rates:last_update", now)
    pipe.execute()
    return {"success": True, "rates_processed": len(payload.rates)}


@app.get("/historical_rates/{symbol}/{date}", dependencies=[Security(verify_token)])
def get_historical_rate(symbol: str, date: str):
    r = get_redis()
    data = r.get(f"historical_rate:{symbol}:{date}")
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No historical rate for {symbol} on {date}",
        )
    return json.loads(data)


@app.get("/historical_rates/{symbol}", dependencies=[Security(verify_token)])
def get_historical_rates_for_symbol(symbol: str):
    r = get_redis()
    dates = sorted(r.smembers(f"historical_rates:{symbol}:dates") or set())
    if not dates:
        return {"symbol": symbol, "count": 0, "rates": []}
    pipe = r.pipeline()
    for d in dates:
        pipe.get(f"historical_rate:{symbol}:{d}")
    rates = [json.loads(b) for b in pipe.execute() if b]
    return {"symbol": symbol, "count": len(rates), "rates": rates}
