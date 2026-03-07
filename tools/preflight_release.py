from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _is_truthy(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _run(cmd: list[str], *, capture: bool = False, verbose: bool = True) -> tuple[int, str]:
    if verbose:
        print(f"$ {' '.join(cmd)}", flush=True)
    if capture:
        completed = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
        out = str(completed.stdout or "").strip()
        err = str(completed.stderr or "").strip()
        combined = out if not err else (f"{out}\n{err}" if out else err)
        return int(completed.returncode), combined

    completed = subprocess.run(cmd, cwd=str(ROOT))
    return int(completed.returncode), ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Run release preflight checks.")
    parser.add_argument("--base", default="http://127.0.0.1:8000", help="backend base URL for smoke check")
    parser.add_argument(
        "--expect-auth-required",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="expect trades endpoint to require auth in smoke check (default: auto from AUTH_ENABLED)",
    )
    parser.add_argument("--json", action="store_true", help="print structured result as JSON")
    args = parser.parse_args()

    explicit_python = str(os.getenv("PREFLIGHT_PYTHON") or "").strip()
    python_cmd = explicit_python or sys.executable

    check_cmd = [python_cmd, "tools/check_release_config.py", "--strict"]
    if args.json:
        check_cmd.append("--json")
    rc, check_output = _run(check_cmd, capture=bool(args.json), verbose=not bool(args.json))

    check_payload: dict | None = None
    if args.json:
        try:
            parsed = json.loads(check_output) if check_output else {}
            check_payload = parsed if isinstance(parsed, dict) else {"raw_output": check_output}
        except Exception:
            check_payload = {"raw_output": check_output}

    if rc != 0:
        if args.json:
            print(
                json.dumps(
                    {
                        "status": "failed",
                        "base": str(args.base),
                        "checks": {"config": check_payload or {"exit_code": rc}},
                        "exit_code": rc,
                    },
                    ensure_ascii=False,
                )
            )
        return rc

    auth_enabled = _is_truthy(os.getenv("AUTH_ENABLED")) or _is_truthy(os.getenv("APP_AUTH_ENABLED"))
    expect_auth_required = bool(args.expect_auth_required) if args.expect_auth_required is not None else auth_enabled

    smoke_cmd = [python_cmd, "tools/smoke_release.py", "--base", str(args.base)]
    if _is_truthy(os.getenv("RATE_LIMIT_ENABLED")) or _is_truthy(os.getenv("APP_RATE_LIMIT_ENABLED")):
        smoke_cmd.append("--expect-rate-limit-headers")
    if not expect_auth_required:
        smoke_cmd.append("--no-expect-auth-required")
    if args.json:
        smoke_cmd.append("--json")

    rc, smoke_output = _run(smoke_cmd, capture=bool(args.json), verbose=not bool(args.json))
    smoke_payload: dict | None = None
    if args.json:
        try:
            parsed = json.loads(smoke_output) if smoke_output else {}
            smoke_payload = parsed if isinstance(parsed, dict) else {"raw_output": smoke_output}
        except Exception:
            smoke_payload = {"raw_output": smoke_output}

    if rc != 0:
        if args.json:
            print(
                json.dumps(
                    {
                        "status": "failed",
                        "base": str(args.base),
                        "checks": {
                            "config": check_payload or {"status": "ok", "exit_code": 0},
                            "smoke": smoke_payload or {"exit_code": rc},
                        },
                        "exit_code": rc,
                    },
                    ensure_ascii=False,
                )
            )
        return rc

    if args.json:
        print(
            json.dumps(
                {
                    "status": "ok",
                    "base": str(args.base),
                    "checks": {
                        "config": check_payload or {"status": "ok", "exit_code": 0},
                        "smoke": smoke_payload or {"status": "ok", "exit_code": 0},
                    },
                    "exit_code": 0,
                },
                ensure_ascii=False,
            )
        )
        return 0

    print("PREFLIGHT: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
