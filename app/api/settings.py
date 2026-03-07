import csv
import io
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select, text
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_session, require_invited_auth
from app.core.config import settings
from app.db.models import Fill, InviteCode, Trade

router = APIRouter(prefix="/settings", tags=["settings"])


def _scoped_user_id(claims: dict) -> Optional[str]:
    if not settings.auth_enabled:
        return None
    sub = str((claims or {}).get("sub") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="invalid auth token: sub is missing")
    return sub


def _try_delete_supabase_auth_user(user_id: str) -> tuple[bool, Optional[str]]:
    base_url = str(settings.supabase_url or "").strip().rstrip("/")
    service_key = str(settings.supabase_service_role_key or "").strip()
    if not base_url or not service_key:
        return False, "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY が未設定のためAuthユーザー削除をスキップしました。"

    url = f"{base_url}/auth/v1/admin/users/{user_id}"
    req = urllib.request.Request(
        url=url,
        method="DELETE",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            return True, None
    except urllib.error.HTTPError as e:
        detail = f"status={e.code}"
        try:
            body = e.read()
            parsed = json.loads(body.decode("utf-8"))
            msg = parsed.get("msg") or parsed.get("message") or parsed.get("error_description")
            if msg:
                detail = f"status={e.code} {msg}"
        except Exception:
            pass
        return False, f"Authユーザー削除に失敗しました（{detail}）"
    except Exception as e:
        return False, f"Authユーザー削除に失敗しました（{e}）"


@router.get("/runtime")
def get_runtime_status(db: Session = Depends(get_session), claims: dict = Depends(require_invited_auth)):
    _scoped_user_id(claims)
    db_status = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        db_status = "ng"

    status = "ok" if db_status == "ok" else "ng"
    return JSONResponse(
        {
            "status": status,
            "db": db_status,
            "auth_enabled": bool(settings.auth_enabled),
            "invite_code_required": bool(settings.invite_code_required),
            "rate_limit_enabled": bool(settings.rate_limit_enabled),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.get("/me")
def get_me(claims: dict = Depends(require_invited_auth)):
    user_id = _scoped_user_id(claims) or "dev-local-user"
    email = str((claims or {}).get("email") or "").strip() or None
    return JSONResponse(
        {
        "user_id": user_id,
        "email": email,
        "auth_enabled": bool(settings.auth_enabled),
        "invite_code_required": bool(settings.invite_code_required),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.get("/export")
def export_my_data(
    format: str = Query(default="json"),
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    scoped_user_id = _scoped_user_id(claims)

    stmt = select(Trade).options(selectinload(Trade.fills))
    if scoped_user_id is not None:
        stmt = stmt.where(Trade.user_id == scoped_user_id)
    trades = list(db.scalars(stmt).all())

    rows = []
    for t in trades:
        buy = next((f for f in t.fills if f.side == "buy"), None)
        sell = next((f for f in t.fills if f.side == "sell"), None)
        rows.append(
            {
                "id": t.id,
                "market": t.market,
                "symbol": t.symbol,
                "name": t.name,
                "status": "open" if not sell else ("complete" if bool(t.review_done) else "pending"),
                "opened_at": t.opened_at,
                "closed_at": t.closed_at or None,
                "buy_date": buy.date if buy else None,
                "buy_price": buy.price if buy else None,
                "buy_qty": buy.qty if buy else None,
                "sell_date": sell.date if sell else None,
                "sell_price": sell.price if sell else None,
                "sell_qty": sell.qty if sell else None,
                "rating": t.rating,
                "tags": t.tags,
                "notes_buy": t.notes_buy,
                "notes_sell": t.notes_sell,
                "notes_review": t.notes_review,
                "review_done": bool(t.review_done),
                "reviewed_at": t.reviewed_at,
                "created_at": t.created_at,
                "updated_at": t.updated_at,
            }
        )

    now_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    clean_format = str(format or "").strip().lower()
    if clean_format == "json":
        body = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(rows),
            "trades": rows,
        }
        return JSONResponse(
            body,
            headers={
                "Content-Disposition": f'attachment; filename="tradetrace_export_{now_tag}.json"',
                "Cache-Control": "no-store",
            },
        )

    if clean_format == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "id",
                "market",
                "symbol",
                "name",
                "status",
                "opened_at",
                "closed_at",
                "buy_date",
                "buy_price",
                "buy_qty",
                "sell_date",
                "sell_price",
                "sell_qty",
                "rating",
                "tags",
                "notes_buy",
                "notes_sell",
                "notes_review",
                "review_done",
                "reviewed_at",
                "created_at",
                "updated_at",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        content = output.getvalue().encode("utf-8-sig")
        return Response(
            content=content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="tradetrace_export_{now_tag}.csv"',
                "Cache-Control": "no-store",
            },
        )

    raise HTTPException(status_code=422, detail="format must be json or csv")


@router.delete("/me")
def delete_my_account_data(
    confirm: bool = Query(default=False),
    confirm_text: str = Query(default=""),
    db: Session = Depends(get_session),
    claims: dict = Depends(require_invited_auth),
):
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required")
    if str(confirm_text or "").strip().upper() != "DELETE":
        raise HTTPException(status_code=400, detail="confirm_text=DELETE is required")

    scoped_user_id = _scoped_user_id(claims)
    if scoped_user_id is None:
        raise HTTPException(status_code=403, detail="auth must be enabled to delete account data")

    to_delete = list(db.scalars(select(Trade.id).where(Trade.user_id == scoped_user_id)).all())
    deleted_count = len(to_delete)
    if deleted_count > 0:
        db.query(Fill).filter(Fill.trade_id.in_(to_delete)).delete(synchronize_session=False)
        db.query(Trade).filter(Trade.id.in_(to_delete)).delete(synchronize_session=False)

    invite_rows = list(
        db.scalars(
            select(InviteCode).where(
                InviteCode.used_by_user_id == scoped_user_id,
                InviteCode.used_count > 0,
            )
        ).all()
    )
    for row in invite_rows:
        # Keep one-time code consumption history, but detach deleted user reference.
        row.used_by_user_id = None
        db.add(row)
    anonymized_invites = len(invite_rows)
    db.commit()

    deleted_auth_user, auth_delete_error = _try_delete_supabase_auth_user(scoped_user_id)
    return JSONResponse(
        {
            "deleted_trades": deleted_count,
            "anonymized_invites": anonymized_invites,
            "deleted_auth_user": deleted_auth_user,
            "auth_delete_error": auth_delete_error,
        },
        headers={"Cache-Control": "no-store"},
    )
