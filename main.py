import json
import os
from datetime import datetime, timezone

import redis
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
API_PORT = int(os.environ.get("API_PORT", 8080))
API_TOKEN = os.environ.get("API_TOKEN", "f118da769ab686ae753192d548f67a3a371904b9d805f4c8fef293cda884fc7f")

app = FastAPI(title="Positions API")

pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
bearer = HTTPBearer()


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


def verify_token(credentials: HTTPAuthorizationCredentials = Security(bearer)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


# ---------- Pydantic models ----------

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


# ---------- Endpoints ----------

@app.get("/health", dependencies=[Security(verify_token)])
def health():
    try:
        r = get_redis()
        r.ping()
        redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {e}"
    return {"api": "ok", "redis": redis_status}


@app.post("/positions", dependencies=[Security(verify_token)])
def post_positions(payload: PositionsPayload):
    r = get_redis()
    raw = payload.model_dump_json()
    now = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set("positions:latest:raw", raw)
    pipe.set("positions:last_update", now)
    for pos in payload.positions:
        pipe.set(f"position:{pos.ticket}", json.dumps(pos.model_dump()))
        pipe.sadd("positions:tickets", str(pos.ticket))
    pipe.execute()

    tickets = [p.ticket for p in payload.positions]
    return {
        "success": True,
        "positions_processed": len(payload.positions),
        "tickets": tickets,
    }


@app.get("/positions/latest", dependencies=[Security(verify_token)])
def get_latest():
    r = get_redis()
    raw = r.get("positions:latest:raw")
    if raw is None:
        raise HTTPException(status_code=404, detail="No positions stored yet")

    last_update = r.get("positions:last_update")
    tickets = list(r.smembers("positions:tickets"))

    individual = {}
    for ticket in tickets:
        data = r.get(f"position:{ticket}")
        if data:
            individual[ticket] = json.loads(data)

    return {
        "last_update": last_update,
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
    for ticket in tickets:
        pipe.get(f"position:{ticket}")
    results = pipe.execute()

    positions = [json.loads(data) for data in results if data]
    positions.sort(key=lambda p: p["ticket"])

    return {"count": len(positions), "positions": positions}
