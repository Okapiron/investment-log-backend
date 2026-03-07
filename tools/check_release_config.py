from pathlib import Path
import argparse
from datetime import datetime, timezone
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import inspect, text

from app.core.config import settings
from app.core.runtime_config import evaluate_runtime_config_issues
from app.db.session import engine


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate release runtime config and schema.")
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="treat warnings as failure (default: false)",
    )
    parser.add_argument("--json", action="store_true", help="print structured result as JSON")
    args = parser.parse_args()

    errors, warnings = evaluate_runtime_config_issues(settings)

    if settings.auth_enabled:
        try:
            with engine.connect() as conn:
                inspector = inspect(conn)
                tables = set(inspector.get_table_names())

                if "trades" not in tables:
                    errors.append("trades table is missing")
                else:
                    trade_cols = {c.get("name") for c in inspector.get_columns("trades")}
                    if "user_id" not in trade_cols:
                        errors.append("trades.user_id is missing (run alembic upgrade head)")

                if settings.invite_code_required:
                    if "invite_codes" not in tables:
                        errors.append("invite_codes table is missing (run alembic upgrade head)")
                    else:
                        invite_cols = {c.get("name") for c in inspector.get_columns("invite_codes")}
                        if "used_at" not in invite_cols:
                            warnings.append("invite_codes.used_at is missing (latest migration not applied)")
                        active_invites = int(
                            conn.execute(
                                text(
                                    """
                                    SELECT COUNT(1)
                                    FROM invite_codes
                                    WHERE expires_at > :now_ts
                                      AND used_count < max_uses
                                    """
                                ),
                                {"now_ts": datetime.now(timezone.utc)},
                            ).scalar()
                            or 0
                        )
                        if active_invites <= 0:
                            warnings.append("no active invite codes found (invite onboarding will be blocked)")
        except Exception as e:
            errors.append(f"database schema check failed: {e}")

    failed = bool(errors) or (bool(args.strict) and bool(warnings))
    exit_code = 1 if failed else 0
    status_label = "FAILED (strict mode)" if (not errors and args.strict and warnings) else ("FAILED" if failed else "OK")

    if args.json:
        payload = {
            "status": "failed" if failed else "ok",
            "strict": bool(args.strict),
            "errors": list(errors),
            "warnings": list(warnings),
            "exit_code": exit_code,
        }
        print(json.dumps(payload, ensure_ascii=False))
        return exit_code

    print(f"CONFIG CHECK: {status_label}")
    for e in errors:
        print(f"- ERROR: {e}")
    for w in warnings:
        print(f"- WARN: {w}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
