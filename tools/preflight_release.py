from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _is_truthy(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _run(cmd: list[str]) -> int:
    print(f"$ {' '.join(cmd)}", flush=True)
    completed = subprocess.run(cmd, cwd=str(ROOT))
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run release preflight checks.")
    parser.add_argument("--base", default="http://127.0.0.1:8000", help="backend base URL for smoke check")
    parser.add_argument(
        "--expect-auth-required",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="expect trades endpoint to require auth in smoke check (default: auto from AUTH_ENABLED)",
    )
    args = parser.parse_args()

    explicit_python = str(os.getenv("PREFLIGHT_PYTHON") or "").strip()
    python_cmd = explicit_python or sys.executable

    rc = _run([python_cmd, "tools/check_release_config.py", "--strict"])
    if rc != 0:
        return rc

    auth_enabled = _is_truthy(os.getenv("AUTH_ENABLED")) or _is_truthy(os.getenv("APP_AUTH_ENABLED"))
    expect_auth_required = bool(args.expect_auth_required) if args.expect_auth_required is not None else auth_enabled

    smoke_cmd = [python_cmd, "tools/smoke_release.py", "--base", str(args.base)]
    if _is_truthy(os.getenv("RATE_LIMIT_ENABLED")) or _is_truthy(os.getenv("APP_RATE_LIMIT_ENABLED")):
        smoke_cmd.append("--expect-rate-limit-headers")
    if not expect_auth_required:
        smoke_cmd.append("--no-expect-auth-required")
    rc = _run(smoke_cmd)
    if rc != 0:
        return rc

    print("PREFLIGHT: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
