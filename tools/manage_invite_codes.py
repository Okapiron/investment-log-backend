from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.invite_admin import classify_invite_code, list_invite_codes, revoke_invite_code
from app.db.session import SessionLocal


def _print_rows(rows: list[dict]) -> None:
    if not rows:
        print("no invite codes")
        return

    print("id | status  | hash_prefix | used | expires_at                 | used_at                    | used_by_user_id")
    print("---+---------+-------------+------+----------------------------+----------------------------+----------------")
    for row in rows:
        used = f"{row['used_count']}/{row['max_uses']}"
        used_by = row["used_by_user_id"] or "-"
        used_at = (row.get("used_at") or "-")[:26]
        print(
            f"{row['id']:>2} | {row['status']:<7} | {row['code_hash_prefix']:<11} | {used:<4} | "
            f"{row['expires_at'][:26]:<26} | {used_at:<26} | {used_by}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage invite codes.")
    sub = parser.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list", help="list invite codes")
    list_parser.add_argument(
        "--status",
        default="all",
        choices=["all", "active", "used", "expired"],
        help="filter by status (default: all)",
    )
    list_parser.add_argument("--limit", type=int, default=50, help="max rows (1-200, default: 50)")
    list_parser.add_argument("--json", action="store_true", help="print list result as JSON")

    revoke_parser = sub.add_parser("revoke", help="revoke an invite code")
    target_group = revoke_parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--id", type=int, help="invite code row id")
    target_group.add_argument("--code", type=str, help="raw invite code")

    args = parser.parse_args()
    with SessionLocal() as db:
        if args.command == "list":
            rows = list_invite_codes(db, status=args.status, limit=args.limit)
            if bool(args.json):
                print(json.dumps(rows, ensure_ascii=False, indent=2))
            else:
                _print_rows(rows)
            return 0

        if args.command == "revoke":
            row = revoke_invite_code(db, invite_id=args.id, code=args.code or "")
            if row is None:
                print("invite code not found")
                return 1
            status = classify_invite_code(row)
            print(f"revoked id={row.id} status={status}")
            return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
