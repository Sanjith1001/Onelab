import argparse
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.analyzer import compute_evaluation_metrics, compute_summary, compute_totals, plain_english_report
from src.config import CURRENCY_SYMBOL, RECON_PERIOD_MONTH, RECON_PERIOD_YEAR
from src.loader import load_bank_settlements, load_platform_transactions
from src.reconciler import reconcile
from src.reporter import print_tables, save_all


def _build_args():
    parser = argparse.ArgumentParser(description="Run payment reconciliation for a target period.")
    parser.add_argument("--month", type=int, default=RECON_PERIOD_MONTH, help="Reconciliation month (1-12)")
    parser.add_argument("--year", type=int, default=RECON_PERIOD_YEAR, help="Reconciliation year")
    return parser.parse_args()


def main():
    args = _build_args()
    logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")

    print("\n" + "=" * 65)
    print("  PayRecon - Payment Reconciliation Engine")
    print(f"  Period: {args.month:02d}/{args.year}")
    print("=" * 65)

    print("\n[step 1/4] Loading data files...")
    platform = load_platform_transactions()
    bank = load_bank_settlements()

    print("\n[step 2/4] Running reconciliation engine...")
    recon = reconcile(platform, bank)
    recon.attrs["period_month"] = args.month
    recon.attrs["period_year"] = args.year

    print("\n[step 3/4] Computing summary...")
    summary = compute_summary(recon)
    totals = compute_totals(recon)

    print(f"\n  Period          : {totals['period']}")
    print(f"  Transactions    : {totals['total_transactions']}")
    print(f"  Matched         : {totals['matched_count']}")
    print(f"  Gaps found      : {totals['gap_count']}")
    print(f"  Platform total  : {CURRENCY_SYMBOL}{totals['total_txn_inr']:,.2f}")
    print(f"  Bank total      : {CURRENCY_SYMBOL}{totals['total_settled_inr']:,.2f}")
    print(f"  Net gap         : {CURRENCY_SYMBOL}{totals['net_gap_inr']:,.5f}")

    print("\n[step 4/4] Generating reports...")
    metrics = compute_evaluation_metrics(recon)
    print_tables(recon, summary, metrics)

    report = plain_english_report(recon)
    print("\n" + report)

    save_all(recon, summary, metrics)

    print("\n[done] Reconciliation complete.\n")


if __name__ == "__main__":
    main()
