from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.deps import get_session
from app.core.errors import raise_409_from_integrity
from app.db.models import Asset
from app.schemas.asset import AssetCreate, AssetRead, AssetUpdate, validate_asset_type

router = APIRouter(prefix="/assets", tags=["assets"])


@router.get("", response_model=list[AssetRead])
def list_assets(
    account_id: Optional[int] = None,
    asset_type: Optional[str] = None,
    is_active: Optional[bool] = True,
    db: Session = Depends(get_session),
):
    stmt = select(Asset).order_by(Asset.display_order, Asset.id)
    if account_id is not None:
        stmt = stmt.where(Asset.account_id == account_id)
    if asset_type is not None:
        stmt = stmt.where(Asset.asset_type == asset_type)
    if is_active is not None:
        stmt = stmt.where(Asset.is_active == is_active)
    return list(db.scalars(stmt).all())


@router.post("", response_model=AssetRead, status_code=201)
def create_asset(payload: AssetCreate, db: Session = Depends(get_session)):
    try:
        validate_asset_type(payload.asset_type)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    obj = Asset(**payload.model_dump())
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.get("/{asset_id}", response_model=AssetRead)
def get_asset(asset_id: int, db: Session = Depends(get_session)):
    obj = db.get(Asset, asset_id)
    if not obj:
        raise HTTPException(status_code=404, detail="asset not found")
    return obj


@router.patch("/{asset_id}", response_model=AssetRead)
def update_asset(asset_id: int, payload: AssetUpdate, db: Session = Depends(get_session)):
    obj = db.get(Asset, asset_id)
    if not obj:
        raise HTTPException(status_code=404, detail="asset not found")
    data = payload.model_dump(exclude_unset=True)
    if "asset_type" in data:
        try:
            validate_asset_type(data["asset_type"])
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
    for k, v in data.items():
        setattr(obj, k, v)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.delete("/{asset_id}", status_code=204)
def delete_asset(asset_id: int, db: Session = Depends(get_session)):
    obj = db.get(Asset, asset_id)
    if not obj:
        raise HTTPException(status_code=404, detail="asset not found")
    db.delete(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    return None
