from datetime import datetime, timedelta, timezone
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select

from app.core.invites import generate_invite_code, hash_invite_code, is_valid_invite_code, normalize_invite_code
from app.db.models import InviteCode
from app.db.session import SessionLocal


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a one-time invite code.")
    parser.add_argument("--days", type=int, default=7, help="expiration in days (default: 7)")
    parser.add_argument("--length", type=int, default=10, help="code length (8-12, default: 10)")
    parser.add_argument("--code", type=str, default="", help="optional fixed code")
    parser.add_argument("--json", action="store_true", help="print output as JSON")
    args = parser.parse_args()

    if args.days <= 0:
        raise SystemExit("--days must be >= 1")

    code = normalize_invite_code(args.code) if args.code else generate_invite_code(args.length)
    if not is_valid_invite_code(code):
        raise SystemExit("invite code must be alphanumeric and 8-12 chars")

    expires_at = datetime.now(timezone.utc) + timedelta(days=args.days)
    code_hash = hash_invite_code(code)

    with SessionLocal() as db:
        exists = db.scalar(select(InviteCode).where(InviteCode.code_hash == code_hash))
        if exists is not None:
            raise SystemExit("same invite code already exists")

        row = InviteCode(
            code_hash=code_hash,
            expires_at=expires_at,
            max_uses=1,
            used_count=0,
            used_by_user_id=None,
        )
        db.add(row)
        db.commit()

    payload = {"invite_code": code, "expires_at": expires_at.isoformat()}
    if bool(args.json):
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"invite_code={code}")
        print(f"expires_at={expires_at.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
