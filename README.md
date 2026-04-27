# Positions API

FastAPI service that receives trading position data and stores it in Redis.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /positions | Store position data |
| POST | /positions/snapshot/begin | Start a staged open-position snapshot |
| POST | /positions/snapshot/{snapshot_id}/chunk | Upload one staged position chunk |
| POST | /positions/snapshot/{snapshot_id}/commit | Replace live open positions from staged chunks |
| GET | /health | API + Redis health check |
| GET | /stats | Redis table counts and last updates |
| GET | /positions/latest | Retrieve latest stored data |
| GET | /closed_positions/months/check | Check whether a date range has synced closed-position months |

## Redis Keys

| Key | Type | Content |
|-----|------|---------|
| `positions:latest:raw` | string | Full raw JSON payload |
| `positions:last_update` | string | ISO timestamp of last POST |
| `position:{ticket}` | string | JSON for individual position |
| `positions:tickets` | set | All known ticket IDs |
| `positions:snapshot:{id}:*` | mixed | Temporary staged open-position snapshot keys |
| `closed_positions:month_meta:{YYYY-MM}` | hash | Monthly closed-position sync metadata |
| `closed_positions:months:synced` | set | Months that have been synced, including zero-row months |

## Setup

```bash
cd /opt/positions-api
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/uvicorn main:app --host 0.0.0.0 --port 8080
```

## Systemd

```bash
cp positions-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable positions-api
systemctl start positions-api
```

## Test

```bash
curl -s http://localhost:8080/health

curl -s -X POST http://localhost:8080/positions \
  -H "Content-Type: application/json" \
  -d '{"positions":[{"ticket":13266182,"login":141924602,"symbol":"EURUSD","cmd":0,"volume":1.0,"open_price":1.085,"price_current":1.0872,"open_time":1769604318,"pnl":22.0,"swap":-1.5,"notional_value":108500.0,"contract_size":100000.0,"profit_currency":"USD"}]}'
```
