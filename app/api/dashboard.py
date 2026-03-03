from typing import Optional

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_session
from app.core.constants import ASSET_TYPES
from app.db.models import Asset, Snapshot
from app.schemas.dashboard import DashboardLatestResponse, DashboardMonthlyResponse, MonthlyPoint

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def month_to_date(month: str) -> datetime:
    return datetime.strptime(month + "-01", "%Y-%m-%d")


def to_month_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def month_range(from_month: str, to_month: str) -> list[str]:
    start = month_to_date(from_month)
    end = month_to_date(to_month)
    months: list[str] = []
    current = start
    while current <= end:
        months.append(to_month_str(current))
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=year, month=month)
    return months


@router.get("/latest", response_model=DashboardLatestResponse)
def dashboard_latest(db: Session = Depends(get_session)):
    latest_month = db.scalar(select(func.max(Snapshot.month)))
    if not latest_month:
        return DashboardLatestResponse(month=None, total_jpy=0, by_asset_type=[])

    rows = db.execute(
        select(Asset.asset_type, func.sum(Snapshot.value_jpy))
        .join(Asset, Asset.id == Snapshot.asset_id)
        .where(Snapshot.month == latest_month)
        .group_by(Asset.asset_type)
    ).all()

    total = sum(int(v or 0) for _, v in rows)
    by_asset_type = []
    for asset_type, value in rows:
        value_int = int(value or 0)
        ratio = (value_int / total) if total else 0
        by_asset_type.append({"asset_type": asset_type, "value_jpy": value_int, "ratio": ratio})

    by_asset_type.sort(key=lambda x: x["value_jpy"], reverse=True)
    return DashboardLatestResponse(month=latest_month, total_jpy=total, by_asset_type=by_asset_type)


@router.get("/monthly", response_model=DashboardMonthlyResponse)
def dashboard_monthly(
    from_: Optional[str] = Query(default=None, alias="from"),
    to: Optional[str] = Query(default=None),
    db: Session = Depends(get_session),
):
    latest_month = db.scalar(select(func.max(Snapshot.month)))
    if not latest_month:
        return DashboardMonthlyResponse(from_=from_, to=to, points=[])

    if to is None:
        to = latest_month
    if from_ is None:
        end = month_to_date(to)
        start_year = end.year - (1 if end.month <= 11 else 0)
        start_month = end.month + 1 if end.month <= 11 else 1
        from_ = f"{start_year:04d}-{start_month:02d}"

    months = month_range(from_, to)

    rows = db.execute(
        select(Snapshot.month, Asset.asset_type, func.sum(Snapshot.value_jpy))
        .join(Asset, Asset.id == Snapshot.asset_id)
        .where(Snapshot.month >= from_, Snapshot.month <= to)
        .group_by(Snapshot.month, Asset.asset_type)
    ).all()

    matrix: dict[str, dict[str, int]] = {
        m: {asset_type: 0 for asset_type in ASSET_TYPES} for m in months
    }
    for month, asset_type, value in rows:
        matrix[month][asset_type] = int(value or 0)

    points: list[MonthlyPoint] = []
    for month in months:
        breakdown = matrix[month]
        points.append(
            MonthlyPoint(month=month, total_jpy=sum(breakdown.values()), by_asset_type=breakdown)
        )

    return DashboardMonthlyResponse(from_=from_, to=to, points=points)
