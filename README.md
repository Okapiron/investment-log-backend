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
