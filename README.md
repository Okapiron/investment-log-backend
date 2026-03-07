“Project home: https://github.com/Okapiron/TradeTrace-meta”

# Backend

## Run
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install '.[dev]'
uvicorn app.main:app --reload
```

## Alembic
```bash
cd backend
alembic upgrade head
```

## Invite Code (manual issue)
```bash
cd backend
.venv/bin/python tools/create_invite_code.py --days 7 --length 10
```

## Auth/CORS/Rate limit env
- `.env.example` を参照

## Release config check
```bash
cd backend
.venv/bin/python tools/check_release_config.py
```

## Ops Runbook
- `docs/public_release_ops.md`
