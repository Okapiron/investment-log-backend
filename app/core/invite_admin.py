from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.invites import hash_invite_code, normalize_invite_code
from app.db.models import InviteCode


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def classify_invite_code(invite: InviteCode, now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    expires_at = _as_utc(invite.expires_at)
    if int(invite.used_count or 0) >= int(invite.max_uses or 1) or bool(invite.used_by_user_id):
        return "used"
    if expires_at <= current:
        return "expired"
    return "active"


def list_invite_codes(
    db: Session,
    *,
    status: str = "all",
    limit: int = 50,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    normalized_status = str(status or "all").strip().lower()
    if normalized_status not in {"all", "active", "used", "expired"}:
        raise ValueError("status must be one of: all, active, used, expired")

    safe_limit = max(1, min(int(limit or 50), 200))
    rows = db.scalars(select(InviteCode).order_by(InviteCode.created_at.desc()).limit(safe_limit)).all()

    current = now or datetime.now(timezone.utc)
    items: list[dict[str, Any]] = []
    for row in rows:
        row_status = classify_invite_code(row, current)
        if normalized_status != "all" and row_status != normalized_status:
            continue
        items.append(
            {
                "id": int(row.id),
                "status": row_status,
                "code_hash_prefix": str(row.code_hash or "")[:10],
                "expires_at": _as_utc(row.expires_at).isoformat(),
                "used_count": int(row.used_count or 0),
                "max_uses": int(row.max_uses or 1),
                "used_by_user_id": row.used_by_user_id,
                "used_at": _as_utc(row.used_at).isoformat() if row.used_at is not None else None,
                "created_at": _as_utc(row.created_at).isoformat(),
            }
        )
    return items


def revoke_invite_code(
    db: Session,
    *,
    invite_id: int | None = None,
    code: str = "",
    now: datetime | None = None,
) -> InviteCode | None:
    row: InviteCode | None = None
    if invite_id is not None:
        row = db.scalar(select(InviteCode).where(InviteCode.id == int(invite_id)))
    else:
        normalized_code = normalize_invite_code(code)
        if not normalized_code:
            raise ValueError("either invite_id or code is required")
        row = db.scalar(select(InviteCode).where(InviteCode.code_hash == hash_invite_code(normalized_code)))

    if row is None:
        return None

    current = now or datetime.now(timezone.utc)
    row.expires_at = current - timedelta(seconds=1)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def purge_invite_codes(
    db: Session,
    *,
    mode: str = "expired",
    older_than_days: int = 30,
    now: datetime | None = None,
) -> int:
    normalized_mode = str(mode or "expired").strip().lower()
    if normalized_mode not in {"expired", "used", "all"}:
        raise ValueError("mode must be one of: expired, used, all")

    days = max(0, int(older_than_days or 0))
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=days)

    stmt = delete(InviteCode).where(InviteCode.created_at <= cutoff)
    if normalized_mode == "expired":
        stmt = stmt.where(InviteCode.expires_at <= current, InviteCode.used_count <= 0)
    elif normalized_mode == "used":
        stmt = stmt.where(InviteCode.used_count > 0)
    else:
        stmt = stmt.where((InviteCode.expires_at <= current) | (InviteCode.used_count > 0))

    result = db.execute(stmt)
    db.commit()
    return int(result.rowcount or 0)
