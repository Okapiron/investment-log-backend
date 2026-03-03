from datetime import datetime

from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.core.constants import ASSET_TYPES
from app.db.base import Base
from app.db.models import Account, Asset, Snapshot
from app.db.session import engine, SessionLocal


def yyyymm(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def seed(db: Session) -> None:
    db.execute(delete(Snapshot))
    db.execute(delete(Asset))
    db.execute(delete(Account))
    db.commit()

    a1 = Account(name="楽天証券", institution="楽天証券", note="NISA", display_order=1, is_active=True)
    a2 = Account(name="住信SBIネット銀行", institution="住信SBIネット銀行", note="生活防衛資金", display_order=2, is_active=True)
    db.add_all([a1, a2])
    db.flush()

    assets = [
        Asset(account_id=a1.id, name="eMAXIS Slim 全世界株式", asset_type="fund", currency="JPY", display_order=1),
        Asset(account_id=a1.id, name="個別株", asset_type="stock", currency="JPY", display_order=2),
        Asset(account_id=a2.id, name="普通預金", asset_type="cash", currency="JPY", display_order=1),
    ]
    db.add_all(assets)
    db.flush()

    now = datetime.now()
    start_year = now.year - (1 if now.month <= 11 else 0)
    start_month = now.month + 1 if now.month <= 11 else 1

    for i in range(12):
        month = (start_month + i - 1) % 12 + 1
        year = start_year + ((start_month + i - 1) // 12)
        m = yyyymm(year, month)

        fund = 12_000_000 + i * 250_000
        stock = 3_000_000 + i * 100_000
        cash = 5_000_000 + i * 50_000

        db.add_all(
            [
                Snapshot(month=m, account_id=a1.id, asset_id=assets[0].id, value_jpy=fund, memo="seed"),
                Snapshot(month=m, account_id=a1.id, asset_id=assets[1].id, value_jpy=stock, memo="seed"),
                Snapshot(month=m, account_id=a2.id, asset_id=assets[2].id, value_jpy=cash, memo="seed"),
            ]
        )

    db.commit()


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        seed(session)
    print("Seed completed")
