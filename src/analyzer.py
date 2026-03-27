import os
import sys

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import (
    CURRENCY_SYMBOL,
    DATASET_NOTE,
    GAP_AGGREGATE_ROUNDING,
    GAP_CROSS_MONTH,
    GAP_DELAYED_SETTLEMENT,
    GAP_DUPLICATE,
    GAP_MATCHED,
    GAP_ORPHAN_REFUND,
    GAP_PARTIAL_SETTLEMENT,
    GAP_ROUNDING,
    GAP_UNMATCHED,
    GAP_UNSETTLED,
    RECON_PERIOD_MONTH,
    RECON_PERIOD_YEAR,
    ASSESSIONS_ASSUMPTIONS,
)


MONTH_NAME = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def compute_summary(recon: pd.DataFrame) -> pd.DataFrame:
    summary_rows = []
    for gap_type, group in recon.groupby("gap_type"):
        summary_rows.append(
            {
                "gap_type": gap_type,
                "transaction_count": len(group),
                "total_txn_amount_inr": round(group["txn_amount_inr"].sum(skipna=True), 2),
                "total_settled_inr": round(group["settled_amount_inr"].sum(skipna=True), 2),
                "net_delta_inr": round(group["delta_inr"].sum(skipna=True), 5),
                "requires_action": gap_type != GAP_MATCHED,
            }
        )

    summary = pd.DataFrame(summary_rows)
    order = {
        GAP_DUPLICATE: 0,
        GAP_ORPHAN_REFUND: 1,
        GAP_DELAYED_SETTLEMENT: 2,
        GAP_CROSS_MONTH: 3,
        GAP_PARTIAL_SETTLEMENT: 4,
        GAP_ROUNDING: 5,
        GAP_AGGREGATE_ROUNDING: 6,
        GAP_UNMATCHED: 7,
        GAP_UNSETTLED: 8,
        GAP_MATCHED: 9,
    }
    summary["_sort"] = summary["gap_type"].map(order).fillna(9)
    summary = summary.sort_values("_sort").drop(columns=["_sort"])
    return summary.reset_index(drop=True)


def compute_totals(recon: pd.DataFrame) -> dict:
    period_month = int(recon.attrs.get("period_month", RECON_PERIOD_MONTH))
    period_year = int(recon.attrs.get("period_year", RECON_PERIOD_YEAR))

    total_txn = recon["txn_amount_inr"].sum(skipna=True)
    total_settled = recon["settled_amount_inr"].sum(skipna=True)
    matched_count = int((recon["gap_type"] == GAP_MATCHED).sum())
    gap_count = int((recon["gap_type"] != GAP_MATCHED).sum())

    return {
        "period": f"{MONTH_NAME[period_month]} {period_year}",
        "period_month": period_month,
        "period_year": period_year,
        "total_transactions": len(recon),
        "matched_count": matched_count,
        "gap_count": gap_count,
        "total_txn_inr": round(total_txn, 2),
        "total_settled_inr": round(total_settled, 2),
        "net_gap_inr": round(total_settled - total_txn, 5),
    }


def compute_evaluation_metrics(recon: pd.DataFrame) -> pd.DataFrame:
    totals = compute_totals(recon)
    total_transactions = max(int(totals["total_transactions"]), 1)
    matched_count = int(totals["matched_count"])
    gap_count = int(totals["gap_count"])
    action_rows = recon[recon["gap_type"] != GAP_MATCHED].copy()
    settled_rows = recon[recon["settlement_date"].notna() & recon["txn_date"].notna()].copy()

    avg_lag = 0.0
    if not settled_rows.empty:
        avg_lag = round(
            (settled_rows["settlement_date"] - settled_rows["txn_date"]).dt.days.mean(),
            2,
        )

    metrics = [
        {
            "metric": "match_rate_pct",
            "value": round((matched_count / total_transactions) * 100, 2),
            "display_value": f"{(matched_count / total_transactions) * 100:.2f}%",
            "status": _threshold_status((matched_count / total_transactions) * 100, good_min=95, warn_min=85),
            "description": "Share of transactions that reconciled cleanly.",
        },
        {
            "metric": "gap_rate_pct",
            "value": round((gap_count / total_transactions) * 100, 2),
            "display_value": f"{(gap_count / total_transactions) * 100:.2f}%",
            "status": _inverse_threshold_status((gap_count / total_transactions) * 100, good_max=3, warn_max=10),
            "description": "Share of transactions requiring operational attention.",
        },
        {
            "metric": "net_exposure_inr",
            "value": float(abs(totals["net_gap_inr"])),
            "display_value": f"{CURRENCY_SYMBOL}{abs(totals['net_gap_inr']):,.5f}",
            "status": _inverse_threshold_status(abs(totals["net_gap_inr"]), good_max=1, warn_max=100),
            "description": "Absolute rupee gap between platform and bank totals.",
        },
        {
            "metric": "duplicate_exposure_inr",
            "value": float(
                abs(recon.loc[recon["gap_type"] == GAP_DUPLICATE, "delta_inr"].sum(skipna=True))
            ),
            "display_value": f"{CURRENCY_SYMBOL}{abs(recon.loc[recon['gap_type'] == GAP_DUPLICATE, 'delta_inr'].sum(skipna=True)):,.5f}",
            "status": _inverse_threshold_status(
                abs(recon.loc[recon["gap_type"] == GAP_DUPLICATE, "delta_inr"].sum(skipna=True)),
                good_max=0,
                warn_max=1000,
            ),
            "description": "Over-settlement exposure caused by duplicate bank payouts.",
        },
        {
            "metric": "avg_settlement_lag_days",
            "value": avg_lag,
            "display_value": f"{avg_lag:.2f} days",
            "status": _inverse_threshold_status(avg_lag, good_max=2, warn_max=4),
            "description": "Average delay between transaction date and settlement date.",
        },
        {
            "metric": "delayed_settlement_count",
            "value": int((recon["gap_type"] == GAP_DELAYED_SETTLEMENT).sum()),
            "display_value": str(int((recon["gap_type"] == GAP_DELAYED_SETTLEMENT).sum())),
            "status": _inverse_threshold_status(int((recon["gap_type"] == GAP_DELAYED_SETTLEMENT).sum()), good_max=0, warn_max=2),
            "description": "Transactions breaching the agreed settlement window.",
        },
        {
            "metric": "orphan_refund_count",
            "value": int((recon["gap_type"] == GAP_ORPHAN_REFUND).sum()),
            "display_value": str(int((recon["gap_type"] == GAP_ORPHAN_REFUND).sum())),
            "status": _inverse_threshold_status(int((recon["gap_type"] == GAP_ORPHAN_REFUND).sum()), good_max=0, warn_max=1),
            "description": "Refunds without a valid original transaction link.",
        },
        {
            "metric": "coverage_rate_pct",
            "value": round(
                (
                    action_rows["in_platform"].fillna(False).sum()
                    + action_rows["in_bank"].fillna(False).sum()
                )
                / (2 * total_transactions)
                * 100,
                2,
            ),
            "display_value": f"{((action_rows['in_platform'].fillna(False).sum() + action_rows['in_bank'].fillna(False).sum()) / (2 * total_transactions) * 100):.2f}%",
            "status": "info",
            "description": "How much of the flagged exception set is observable on both sides of the reconciliation.",
        },
    ]

    return pd.DataFrame(metrics)


def plain_english_report(recon: pd.DataFrame) -> str:
    totals = compute_totals(recon)
    lines = [
        "=" * 65,
        f"  RECONCILIATION REPORT - {totals['period']}",
        "=" * 65,
        f"  Total transactions   : {totals['total_transactions']}",
        f"  Matched and clean    : {totals['matched_count']}",
        f"  Require attention    : {totals['gap_count']}",
        f"  Platform total       : {CURRENCY_SYMBOL}{totals['total_txn_inr']:,.2f}",
        f"  Bank total           : {CURRENCY_SYMBOL}{totals['total_settled_inr']:,.2f}",
        f"  Net gap              : {CURRENCY_SYMBOL}{totals['net_gap_inr']:,.5f}",
        "=" * 65,
        "",
    ]

    issues = []
    for gap in [
        GAP_DUPLICATE,
        GAP_ORPHAN_REFUND,
        GAP_DELAYED_SETTLEMENT,
        GAP_CROSS_MONTH,
        GAP_PARTIAL_SETTLEMENT,
        GAP_ROUNDING,
        GAP_AGGREGATE_ROUNDING,
        GAP_UNMATCHED,
        GAP_UNSETTLED,
    ]:
        group = recon[recon["gap_type"] == gap]
        if group.empty:
            continue
        issues.append(_describe_gap(gap, group))

    if not issues:
        lines.append("  All transactions matched. No issues found.")
        return "\n".join(lines)

    lines.append("  ISSUES FOUND")
    lines.append("")
    lines.extend(issues)
    lines.append("=" * 65)
    lines.append("  PRODUCTION NOTES")
    lines.append("=" * 65)
    lines.append(f"  - {DATASET_NOTE}")
    lines.append("  - Store money in paise, not float, to avoid hidden drift.")
    lines.append("  - Re-run reconciliation with a rolling lookback because bank files can arrive late.")
    lines.append("  - Review timezone assumptions before month-end reporting.")
    lines.append("")
    lines.append("  ASSUMPTIONS")
    for assumption in ASSESSIONS_ASSUMPTIONS:
        lines.append(f"  - {assumption}")
    return "\n".join(lines)


def _describe_gap(gap: str, group: pd.DataFrame) -> str:
    if gap == GAP_DUPLICATE:
        row = group.iloc[0]
        return (
            f"  DUPLICATE ({len(group)} row/s)\n"
            f"  Transaction {row['transaction_id']} appears over-settled and should be disputed with the bank.\n"
        )
    if gap == GAP_ORPHAN_REFUND:
        row = group.iloc[0]
        return (
            f"  ORPHAN_REFUND ({len(group)} row/s)\n"
            f"  Refund {row['transaction_id']} has no valid original transaction link and needs review.\n"
        )
    if gap == GAP_DELAYED_SETTLEMENT:
        row = group.iloc[0]
        return (
            f"  DELAYED_SETTLEMENT ({len(group)} row/s)\n"
            f"  Transaction {row['transaction_id']} settled after the allowed window.\n"
        )
    if gap == GAP_CROSS_MONTH:
        row = group.iloc[0]
        return (
            f"  CROSS_MONTH ({len(group)} row/s)\n"
            f"  Transaction {row['transaction_id']} belongs to one month but settled in another.\n"
        )
    if gap == GAP_PARTIAL_SETTLEMENT:
        row = group.iloc[0]
        return (
            f"  PARTIAL_SETTLEMENT ({len(group)} row/s)\n"
            f"  Transaction {row['transaction_id']} settled across multiple bank entries.\n"
        )
    if gap == GAP_ROUNDING:
        row = group.iloc[0]
        return (
            f"  ROUNDING_DIFF ({len(group)} row/s)\n"
            f"  Small decimal drift exists; sample transaction {row['transaction_id']} differs by {row['delta_inr']:+.5f}.\n"
        )
    if gap == GAP_AGGREGATE_ROUNDING:
        row = group.iloc[0]
        return (
            f"  AGGREGATE_ROUNDING_ISSUE ({len(group)} row/s)\n"
            f"  Rounding-only rows net to {row['delta_inr']:+.5f}, which should be posted separately.\n"
        )
    if gap == GAP_UNMATCHED:
        return (
            f"  UNMATCHED ({len(group)} row/s)\n"
            f"  Bank settlements exist without a platform transaction.\n"
        )
    return (
        f"  UNSETTLED ({len(group)} row/s)\n"
        f"  Platform transactions exist without a bank settlement.\n"
    )


def _threshold_status(value: float, good_min: float, warn_min: float) -> str:
    if value >= good_min:
        return "good"
    if value >= warn_min:
        return "warn"
    return "bad"


def _inverse_threshold_status(value: float, good_max: float, warn_max: float) -> str:
    if value <= good_max:
        return "good"
    if value <= warn_max:
        return "warn"
    return "bad"


if __name__ == "__main__":
    from src.loader import load_bank_settlements, load_platform_transactions
    from src.reconciler import reconcile

    platform = load_platform_transactions()
    bank = load_bank_settlements()
    recon = reconcile(platform, bank)

    print(compute_summary(recon).to_string(index=False))
    print()
    print(compute_evaluation_metrics(recon).to_string(index=False))
    print()
    print(plain_english_report(recon))
