from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.deps import get_session
from app.core.errors import raise_409_from_integrity
from app.db.models import Account
from app.schemas.account import AccountCreate, AccountRead, AccountUpdate

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("", response_model=list[AccountRead])
def list_accounts(is_active: Optional[bool] = True, db: Session = Depends(get_session)):
    stmt = select(Account).order_by(Account.display_order, Account.id)
    if is_active is not None:
        stmt = stmt.where(Account.is_active == is_active)
    return list(db.scalars(stmt).all())


@router.post("", response_model=AccountRead, status_code=201)
def create_account(payload: AccountCreate, db: Session = Depends(get_session)):
    obj = Account(**payload.model_dump())
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.get("/{account_id}", response_model=AccountRead)
def get_account(account_id: int, db: Session = Depends(get_session)):
    obj = db.get(Account, account_id)
    if not obj:
        raise HTTPException(status_code=404, detail="account not found")
    return obj


@router.patch("/{account_id}", response_model=AccountRead)
def update_account(account_id: int, payload: AccountUpdate, db: Session = Depends(get_session)):
    obj = db.get(Account, account_id)
    if not obj:
        raise HTTPException(status_code=404, detail="account not found")
    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(obj, k, v)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    db.refresh(obj)
    return obj


@router.delete("/{account_id}", status_code=204)
def delete_account(account_id: int, db: Session = Depends(get_session)):
    obj = db.get(Account, account_id)
    if not obj:
        raise HTTPException(status_code=404, detail="account not found")
    db.delete(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise_409_from_integrity(e)
    return None
