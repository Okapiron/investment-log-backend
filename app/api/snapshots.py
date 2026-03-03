from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.deps import get_session
from app.core.errors import raise_409_from_integrity
from app.db.models import Asset, Snapshot
from app.schemas.snapshot import SnapshotCreate, SnapshotRead, SnapshotUpdate
from app.schemas.monthly import CopyLatestRequest, CopyLatestResponse

router = APIRouter(prefix="/snapshots", tags=["snapshots"])


@router.get("", response_model=list[SnapshotRead])
def list_snapshots(
    month: Optional[str] = None,
    account_id: Optional[int] = None,
    asset_id: Optional[int] = None,
    db: Session = Depends(get_session),
):
    stmt = select(Snapshot).order_by(Snapshot.month, Snapshot.id)
    if month is not None:
        stmt = stmt.where(Snapshot.month == month)
    if account_id is not None:
        stmt = stmt.where(Snapshot.account_id == account_id)
    if asset_id is not None:
        stmt = stmt.where(Snapshot.asset_id == asset_id)
    return list(db.scalars(stmt).all())


@router.post("", response_model=SnapshotRead, status_code=201)
def create_snapshot(payload: SnapshotCreate, db: Session = Depends(get_session)):
    asset = db.get(Asset, payload.asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="asset not found")

    obj = Snapshot(
        month=payload.month,
        asset_id=payload.asset_id,
        account_id=asset.account_id,
        value_jpy=payload.value_jpy,
        memo=payload.memo,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.get("/{snapshot_id}", response_model=SnapshotRead)
def get_snapshot(snapshot_id: int, db: Session = Depends(get_session)):
    obj = db.get(Snapshot, snapshot_id)
    if not obj:
        raise HTTPException(status_code=404, detail="snapshot not found")
    return obj


@router.patch("/{snapshot_id}", response_model=SnapshotRead)
def update_snapshot(snapshot_id: int, payload: SnapshotUpdate, db: Session = Depends(get_session)):
    obj = db.get(Snapshot, snapshot_id)
    if not obj:
        raise HTTPException(status_code=404, detail="snapshot not found")

    data = payload.model_dump(exclude_unset=True)
    if "asset_id" in data:
        asset = db.get(Asset, data["asset_id"])
        if not asset:
            raise HTTPException(status_code=404, detail="asset not found")
        obj.asset_id = asset.id
        obj.account_id = asset.account_id

    for k in ("month", "value_jpy", "memo"):
        if k in data:
            setattr(obj, k, data[k])

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.delete("/{snapshot_id}", status_code=204)
def delete_snapshot(snapshot_id: int, db: Session = Depends(get_session)):
    obj = db.get(Snapshot, snapshot_id)
    if not obj:
        raise HTTPException(status_code=404, detail="snapshot not found")
    db.delete(obj)
    db.commit()
    return None


@router.post("/copy-latest", response_model=CopyLatestResponse)
def copy_latest_snapshot(payload: CopyLatestRequest, db: Session = Depends(get_session)):
    to_month = payload.to_month
    if len(to_month) != 7 or to_month[4] != "-":
        raise HTTPException(status_code=400, detail="to_month must be in YYYY-MM format")

    from_month = db.scalar(select(func.max(Snapshot.month)))
    if not from_month:
        raise HTTPException(status_code=400, detail="no snapshots found")

    source_rows = db.execute(
        select(Asset.id, Asset.account_id, Snapshot.value_jpy, Snapshot.memo)
        .join(Snapshot, Snapshot.asset_id == Asset.id)
        .where(Snapshot.month == from_month, Asset.is_active == True)
    ).all()

    existing_asset_ids = set(
        db.scalars(
            select(Snapshot.asset_id).where(Snapshot.month == to_month)
        ).all()
    )

    created = 0
    skipped = 0

    for asset_id, account_id, value_jpy, memo in source_rows:
        if asset_id in existing_asset_ids:
            skipped += 1
            continue

        db.add(
            Snapshot(
                month=to_month,
                account_id=account_id,
                asset_id=asset_id,
                value_jpy=value_jpy,
                memo=memo,
            )
        )
        existing_asset_ids.add(asset_id)
        created += 1

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)

    return CopyLatestResponse(from_month=from_month, to_month=to_month, created=created, skipped=skipped)
