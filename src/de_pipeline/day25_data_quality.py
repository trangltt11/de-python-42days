from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    severity: str  # "ERROR" | "WARN"
    details: dict[str, Any]


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def find_one_parquet_partition(processed_root: Path) -> Path:
    # Tìm 1 file events.parquet trong processed_root/date=.../
    parts = sorted([p for p in processed_root.glob("date=*/events.parquet")])
    if not parts:
        raise FileNotFoundError(f"Cannot find date=*/events.parquet under: {processed_root}")
    return parts[0]


# ---------- Checks ----------

def check_required_columns(df: pd.DataFrame, required: list[str]) -> CheckResult:
    missing = [c for c in required if c not in df.columns]
    passed = len(missing) == 0
    return CheckResult(
        name="required_columns",
        passed=passed,
        severity="ERROR",
        details={"missing_columns": missing, "required": required},
    )


def check_null_rate(df: pd.DataFrame, columns: list[str], max_null_rate: float) -> CheckResult:
    total = len(df)
    nulls = df[columns].isna().sum().to_dict()
    rates = {k: (nulls[k] / total if total else 0.0) for k in nulls}
    bad = {k: v for k, v in rates.items() if v > max_null_rate}
    passed = len(bad) == 0
    return CheckResult(
        name="null_rate",
        passed=passed,
        severity="ERROR",
        details={"max_null_rate": max_null_rate, "rates": rates, "violations": bad},
    )


def check_unique_key(df: pd.DataFrame, key: str) -> CheckResult:
    dup_count = int(df.duplicated(subset=[key]).sum())
    passed = dup_count == 0
    return CheckResult(
        name="unique_key",
        passed=passed,
        severity="ERROR",
        details={"key": key, "duplicate_rows": dup_count},
    )


def check_event_domain(df: pd.DataFrame, allowed: set[str]) -> CheckResult:
    values = df["event"].astype(str).str.strip().str.lower()
    invalid = sorted(set(values.unique()) - allowed)
    passed = len(invalid) == 0
    return CheckResult(
        name="event_domain",
        passed=passed,
        severity="ERROR",
        details={"allowed": sorted(allowed), "invalid_values": invalid},
    )


def check_amount_rules(df: pd.DataFrame) -> CheckResult:
    # Ensure numeric
    amt = pd.to_numeric(df["amount"], errors="coerce")
    ev = df["event"].astype(str).str.strip().str.lower()

    # rule checks
    bad_purchase = int(((ev == "purchase") & (amt <= 0)).sum())
    bad_view = int(((ev == "view") & (amt != 0)).sum())
    bad_refund = int(((ev == "refund") & (amt >= 0)).sum())

    passed = (bad_purchase + bad_view + bad_refund) == 0
    return CheckResult(
        name="amount_rules",
        passed=passed,
        severity="ERROR",
        details={
            "bad_purchase_amount_le_0": bad_purchase,
            "bad_view_amount_ne_0": bad_view,
            "bad_refund_amount_ge_0": bad_refund,
        },
    )


def check_fk_users(df: pd.DataFrame, users_csv: Path) -> CheckResult:
    users = pd.read_csv(users_csv)
    users["user_id"] = users["user_id"].astype(str).str.strip()
    known = set(users["user_id"].dropna().tolist())

    df_users = df["user_id"].astype(str).str.strip()
    missing_users = sorted(set(df_users.unique()) - known)

    # Đây thường là WARN (tuỳ hệ thống). Bạn có thể nâng lên ERROR nếu muốn.
    passed = len(missing_users) == 0
    return CheckResult(
        name="fk_users",
        passed=passed,
        severity="WARN",
        details={"missing_user_ids": missing_users, "users_csv": str(users_csv)},
    )


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- Runner ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Day25 Data Quality Checks")

    p.add_argument("--processed-root", default="data/processed/day15",
                  help="Processed root containing date=*/events.parquet (relative to project root)")
    p.add_argument("--users-csv", default="data/raw/day9_users.csv",
                  help="Users dimension csv for FK check (relative to project root)")
    p.add_argument("--output", default="data/processed/day25/dq_report.json",
                  help="Output report path (relative to project root)")
    p.add_argument("--max-null-rate", type=float, default=0.0,
                  help="Max allowed null rate per selected column (0.0 means no null allowed)")

    return p.parse_args()


def main() -> None:
    root = project_root()
    args = parse_args()

    processed_root = root / args.processed_root
    users_csv = root / args.users_csv
    out_report = root / args.output

    parquet_path = find_one_parquet_partition(processed_root)
    df = pd.read_parquet(parquet_path)

    required = ["event_id", "user_id", "event", "amount", "ts"]
    allowed_events = {"view", "purchase", "refund"}

    checks: list[CheckResult] = []
    checks.append(check_required_columns(df, required))
    checks.append(check_null_rate(df, ["event_id", "user_id", "event"], max_null_rate=args.max_null_rate))
    checks.append(check_unique_key(df, "event_id"))
    checks.append(check_event_domain(df, allowed_events))
    checks.append(check_amount_rules(df))
    if users_csv.exists():
        checks.append(check_fk_users(df, users_csv))
    else:
        checks.append(CheckResult(
            name="fk_users",
            passed=True,
            severity="WARN",
            details={"skipped": True, "reason": f"users_csv not found: {users_csv}"},
        ))

    # overall status: fail nếu có ERROR không passed
    error_fails = [c for c in checks if c.severity == "ERROR" and not c.passed]
    warn_fails = [c for c in checks if c.severity == "WARN" and not c.passed]

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": {
            "processed_root": str(processed_root),
            "parquet_path": str(parquet_path),
            "rows": int(len(df)),
            "cols": list(df.columns),
        },
        "summary": {
            "status": "PASS" if len(error_fails) == 0 else "FAIL",
            "error_failed": [c.name for c in error_fails],
            "warn_failed": [c.name for c in warn_fails],
        },
        "checks": [
            {"name": c.name, "passed": c.passed, "severity": c.severity, "details": c.details}
            for c in checks
        ],
    }

    write_report(out_report, report)

    print("DQ status:", report["summary"]["status"])
    print("Report:", out_report)
    if error_fails:
        print("ERROR failed:", [c.name for c in error_fails])
    if warn_fails:
        print("WARN failed:", [c.name for c in warn_fails])


if __name__ == "__main__":
    main()