from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.invite_admin import classify_invite_code, list_invite_codes, purge_invite_codes, revoke_invite_code
from app.core.invites import hash_invite_code
from app.db.base import Base
from app.db.models import InviteCode


def _build_session():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def test_list_invite_codes_with_status_filters():
    SessionLocal = _build_session()
    now = datetime.now(timezone.utc)

    with SessionLocal() as db:
        db.add_all(
            [
                InviteCode(
                    code_hash=hash_invite_code("ACTIVE1234"),
                    expires_at=now + timedelta(days=3),
                    max_uses=1,
                    used_count=0,
                ),
                InviteCode(
                    code_hash=hash_invite_code("USED123456"),
                    expires_at=now + timedelta(days=3),
                    max_uses=1,
                    used_count=1,
                    used_by_user_id="user-1",
                    used_at=now - timedelta(minutes=10),
                ),
                InviteCode(
                    code_hash=hash_invite_code("EXPIRED123"),
                    expires_at=now - timedelta(days=1),
                    max_uses=1,
                    used_count=0,
                ),
            ]
        )
        db.commit()

        all_rows = list_invite_codes(db, status="all", limit=50, now=now)
        statuses = {row["status"] for row in all_rows}
        assert statuses == {"active", "used", "expired"}

        active_rows = list_invite_codes(db, status="active", limit=50, now=now)
        assert len(active_rows) == 1
        assert active_rows[0]["status"] == "active"

        used_rows = list_invite_codes(db, status="used", limit=50, now=now)
        assert len(used_rows) == 1
        assert used_rows[0]["status"] == "used"
        assert used_rows[0]["used_at"] is not None

        expired_rows = list_invite_codes(db, status="expired", limit=50, now=now)
        assert len(expired_rows) == 1
        assert expired_rows[0]["status"] == "expired"


def test_revoke_invite_code_by_code_and_id():
    SessionLocal = _build_session()
    now = datetime.now(timezone.utc)

    with SessionLocal() as db:
        target = InviteCode(
            code_hash=hash_invite_code("REVKCODE01"),
            expires_at=now + timedelta(days=2),
            max_uses=1,
            used_count=0,
        )
        target2 = InviteCode(
            code_hash=hash_invite_code("REVKCODE02"),
            expires_at=now + timedelta(days=2),
            max_uses=1,
            used_count=0,
        )
        db.add_all([target, target2])
        db.commit()
        db.refresh(target)
        db.refresh(target2)

        revoked = revoke_invite_code(db, code="REVKCODE01", now=now)
        assert revoked is not None
        assert classify_invite_code(revoked, now=now) == "expired"

        revoked_by_id = revoke_invite_code(db, invite_id=target2.id, now=now)
        assert revoked_by_id is not None
        assert classify_invite_code(revoked_by_id, now=now) == "expired"


def test_revoke_invite_code_not_found_returns_none():
    SessionLocal = _build_session()
    with SessionLocal() as db:
        assert revoke_invite_code(db, code="NOCODE999") is None


def test_purge_invite_codes_by_mode_and_age():
    SessionLocal = _build_session()
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=40)

    with SessionLocal() as db:
        db.add_all(
            [
                InviteCode(
                    code_hash=hash_invite_code("OLDXP0001"),
                    expires_at=now - timedelta(days=10),
                    max_uses=1,
                    used_count=0,
                    created_at=old,
                    updated_at=old,
                ),
                InviteCode(
                    code_hash=hash_invite_code("OLDUSD001"),
                    expires_at=now + timedelta(days=10),
                    max_uses=1,
                    used_count=1,
                    used_by_user_id="u1",
                    created_at=old,
                    updated_at=old,
                ),
                InviteCode(
                    code_hash=hash_invite_code("NEWXP0001"),
                    expires_at=now - timedelta(days=1),
                    max_uses=1,
                    used_count=0,
                    created_at=now - timedelta(days=5),
                    updated_at=now - timedelta(days=5),
                ),
            ]
        )
        db.commit()

        deleted_expired = purge_invite_codes(db, mode="expired", older_than_days=30, now=now)
        assert deleted_expired == 1

        deleted_used = purge_invite_codes(db, mode="used", older_than_days=30, now=now)
        assert deleted_used == 1

        deleted_all = purge_invite_codes(db, mode="all", older_than_days=0, now=now)
        assert deleted_all == 1


def test_purge_invite_codes_dry_run_keeps_data():
    SessionLocal = _build_session()
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=45)

    with SessionLocal() as db:
        db.add(
            InviteCode(
                code_hash=hash_invite_code("DRYRUN001"),
                expires_at=now - timedelta(days=5),
                max_uses=1,
                used_count=0,
                created_at=old,
                updated_at=old,
            )
        )
        db.commit()

        preview = purge_invite_codes(db, mode="expired", older_than_days=30, dry_run=True, now=now)
        assert preview == 1

        remains = db.scalar(select(InviteCode).where(InviteCode.code_hash == hash_invite_code("DRYRUN001")))
        assert remains is not None
