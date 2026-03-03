from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError


def raise_409_from_integrity(error: IntegrityError) -> None:
    detail = str(error.orig)
    if "UNIQUE constraint failed" in detail:
        raise HTTPException(status_code=409, detail="unique constraint violated")
    if "FOREIGN KEY constraint failed" in detail:
        raise HTTPException(status_code=409, detail="referential integrity violated")
    if "CHECK constraint failed" in detail:
        raise HTTPException(status_code=409, detail="check constraint violated")
    raise HTTPException(status_code=409, detail="integrity constraint violated")
