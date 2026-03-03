from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.api.deps import get_session
from app.db.models import Account, Asset, Snapshot
from app.schemas.monthly import MonthlyAccountRow, MonthlyAssetRow, MonthlyResponse, MonthlySummary

router = APIRouter(prefix="/monthly", tags=["monthly"])


@router.get("", response_model=MonthlyResponse)
def get_monthly_tree(
    month: str = Query(..., min_length=7, max_length=7),
    db: Session = Depends(get_session),
):
    if len(month) != 7 or month[4] != "-":
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format")

    account_objs = list(
        db.scalars(
            select(Account)
            .where(Account.is_active == True)
            .order_by(Account.display_order, Account.id)
        ).all()
    )

    account_map = {
        account.id: MonthlyAccountRow(account_id=account.id, account_name=account.name, assets=[])
        for account in account_objs
    }

    rows = db.execute(
        select(
            Asset.account_id,
            Asset.id,
            Asset.name,
            Asset.asset_type,
            Asset.currency,
            Snapshot.id,
            Snapshot.value_jpy,
        )
        .join(Account, Account.id == Asset.account_id)
        .outerjoin(Snapshot, and_(Snapshot.asset_id == Asset.id, Snapshot.month == month))
        .where(Account.is_active == True, Asset.is_active == True)
        .order_by(Asset.account_id, Asset.display_order, Asset.id)
    ).all()

    filled = 0
    missing = 0

    for account_id, asset_id, asset_name, asset_type, currency, snapshot_id, value_jpy in rows:
        if account_id not in account_map:
            continue

        value: Optional[int] = int(value_jpy) if value_jpy is not None else None
        snap_id: Optional[int] = int(snapshot_id) if snapshot_id is not None else None

        if value is None:
            missing += 1
        else:
            filled += 1

        account_map[account_id].assets.append(
            MonthlyAssetRow(
                asset_id=int(asset_id),
                asset_name=asset_name,
                asset_type=asset_type,
                currency=currency,
                value_jpy=value,
                snapshot_id=snap_id,
            )
        )

    return MonthlyResponse(
        month=month,
        accounts=list(account_map.values()),
        summary=MonthlySummary(filled=filled, missing=missing),
    )
