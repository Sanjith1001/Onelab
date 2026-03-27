import logging
import os
import sys
from collections import defaultdict
from decimal import Decimal

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import (
    AGGREGATE_ROUNDING_THRESHOLD,
    COL_BANK_REF,
    COL_REFUND_ORIGINAL_TXN_ID,
    COL_SETTLE_AMOUNT,
    COL_SETTLE_DATE,
    COL_SETTLE_ID,
    COL_TXN_AMOUNT,
    COL_TXN_DATE,
    COL_TXN_ID,
    COL_TXN_REF,
    COL_TXN_TYPE,
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
    PAISE_FACTOR,
    ROUNDING_EPSILON,
    SETTLEMENT_WINDOW_DAYS,
)


logger = logging.getLogger("payrecon.reconciler")


def reconcile(platform: pd.DataFrame, bank: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting reconciliation engine")

    platform, bank = _ensure_money_columns(platform, bank)
    bank_agg = _aggregate_bank_rows(bank)
    refund_context = _build_refund_context(platform)

    merged = pd.merge(
        platform,
        bank_agg,
        left_on=COL_TXN_ID,
        right_on=COL_TXN_REF,
        how="outer",
        suffixes=("_platform", "_bank"),
    )

    logger.info("merge completed with %s rows", len(merged))

    merged["gap_type"] = merged.apply(lambda row: _classify_row(row, refund_context), axis=1)
    merged["delta_paise"] = merged.apply(_compute_delta_paise, axis=1)
    merged["delta_inr"] = merged.apply(_compute_delta_inr, axis=1)
    merged["in_platform"] = merged[COL_TXN_ID].notna()
    merged["in_bank"] = merged[COL_TXN_REF].notna()

    result = _build_output(merged)
    result = _append_aggregate_rounding_issue(result, merged)

    counts = result["gap_type"].value_counts()
    logger.info("classification results")
    for gap, count in counts.items():
        logger.info("%s : %s rows", f"{gap:<25}", count)

    return result


def _aggregate_bank_rows(bank: pd.DataFrame) -> pd.DataFrame:
    bank = bank.copy()

    bank["is_duplicate_by_ref"] = bank.duplicated(subset=[COL_TXN_REF], keep=False)
    amount_date_stats = (
        bank.groupby([COL_SETTLE_DATE, "settled_amount_paise"], dropna=False)
        .agg(
            amount_date_row_count=(COL_SETTLE_ID, "count"),
            amount_date_unique_refs=(COL_TXN_REF, "nunique"),
        )
        .reset_index()
    )
    bank = bank.merge(
        amount_date_stats,
        on=[COL_SETTLE_DATE, "settled_amount_paise"],
        how="left",
    )
    bank["is_duplicate_amount_date"] = (
        bank["amount_date_row_count"].fillna(0).astype(int).gt(1)
        & bank["amount_date_unique_refs"].fillna(0).astype(int).gt(1)
    )

    grouped = bank.groupby(COL_TXN_REF, dropna=False).agg(
        settlement_count=(COL_SETTLE_ID, "count"),
        settlement_id=(COL_SETTLE_ID, lambda s: "|".join(sorted(set(s.dropna().astype(str))))),
        settlement_date=(COL_SETTLE_DATE, "max"),
        first_settlement_date=(COL_SETTLE_DATE, "min"),
        settled_amount_paise=("settled_amount_paise", "sum"),
        settled_amount_inr=(COL_SETTLE_AMOUNT, "sum"),
        settled_amount_raw_decimal=("settled_amount_raw_decimal", lambda s: sum(s, Decimal("0"))),
        bank_reference=(COL_BANK_REF, lambda s: "|".join(sorted(set(s.dropna().astype(str))))),
        is_duplicate_by_ref=("is_duplicate_by_ref", "any"),
        is_duplicate_amount_date=("is_duplicate_amount_date", "any"),
        amount_date_row_count=("amount_date_row_count", "max"),
        amount_date_unique_refs=("amount_date_unique_refs", "max"),
    ).reset_index()

    grouped["is_bank_duplicate"] = grouped["is_duplicate_by_ref"] | grouped["is_duplicate_amount_date"]
    dup_count = int(grouped["is_bank_duplicate"].sum())
    if dup_count > 0:
        dup_ids = grouped.loc[grouped["is_bank_duplicate"], COL_TXN_REF].tolist()
        logger.info("duplicates flagged for %s bank refs: %s", dup_count, dup_ids)

    return grouped


def _classify_row(row, refund_context: dict[str, object]) -> str:
    in_platform = pd.notna(row.get(COL_TXN_ID))
    in_bank = pd.notna(row.get(COL_TXN_REF))

    if in_platform and not in_bank:
        txn_type = str(row.get(COL_TXN_TYPE, "")).upper()
        amount_paise = row.get("txn_amount_paise", np.nan)
        if txn_type == "REFUND" or (pd.notna(amount_paise) and int(amount_paise) < 0):
            return GAP_UNSETTLED if _is_valid_refund(row, refund_context) else GAP_ORPHAN_REFUND
        return GAP_UNSETTLED

    if in_bank and bool(row.get("is_duplicate_amount_date", False)):
        return GAP_DUPLICATE

    if not in_platform and in_bank:
        return GAP_UNMATCHED

    txn_amt_paise = row.get("txn_amount_paise", np.nan)
    settled_amt_paise = row.get("settled_amount_paise", np.nan)
    settlement_count = int(row.get("settlement_count", 1) or 1)
    txn_date = row.get(COL_TXN_DATE)
    settle_date = row.get(COL_SETTLE_DATE)

    if pd.notna(txn_date) and pd.notna(settle_date):
        if (settle_date - txn_date).days > SETTLEMENT_WINDOW_DAYS:
            return GAP_DELAYED_SETTLEMENT

    if settlement_count > 1 and pd.notna(txn_amt_paise) and pd.notna(settled_amt_paise):
        if int(settled_amt_paise) == int(txn_amt_paise):
            return GAP_PARTIAL_SETTLEMENT

    if bool(row.get("is_duplicate_by_ref", False)) and pd.notna(txn_amt_paise) and pd.notna(settled_amt_paise):
        if int(settled_amt_paise) >= int(txn_amt_paise):
            return GAP_DUPLICATE

    if pd.notna(txn_date) and pd.notna(settle_date):
        if txn_date.month != settle_date.month or txn_date.year != settle_date.year:
            return GAP_CROSS_MONTH

    if pd.notna(txn_amt_paise) and pd.notna(settled_amt_paise):
        paise_delta = abs(int(settled_amt_paise) - int(txn_amt_paise))
        raw_delta = abs(_raw_decimal_delta(row))
        if raw_delta != Decimal("0") and paise_delta <= int(ROUNDING_EPSILON * PAISE_FACTOR):
            return GAP_ROUNDING
        if paise_delta > int(ROUNDING_EPSILON * PAISE_FACTOR):
            return GAP_UNMATCHED

    return GAP_MATCHED


def _compute_delta_paise(row):
    txn_amt = row.get("txn_amount_paise")
    settle_amt = row.get("settled_amount_paise")
    if pd.isna(txn_amt) or pd.isna(settle_amt):
        return np.nan
    return int(settle_amt) - int(txn_amt)


def _compute_delta_inr(row):
    raw_delta = _raw_decimal_delta(row)
    if raw_delta != Decimal("0"):
        return round(float(raw_delta), 5)

    paise_delta = _compute_delta_paise(row)
    if pd.isna(paise_delta):
        return np.nan
    return round(float(paise_delta) / PAISE_FACTOR, 5)


def _raw_decimal_delta(row) -> Decimal:
    txn_raw = row.get("txn_amount_raw_decimal")
    settle_raw = row.get("settled_amount_raw_decimal")
    if pd.isna(txn_raw) or pd.isna(settle_raw):
        return Decimal("0")
    return Decimal(str(settle_raw)) - Decimal(str(txn_raw))


def _build_refund_context(platform: pd.DataFrame) -> dict[str, object]:
    normalized_ids = platform[COL_TXN_ID].dropna().astype(str).str.strip().str.upper()
    original_ids = set(normalized_ids)
    original_amount_map = (
        platform.set_index(COL_TXN_ID)["txn_amount_paise"].to_dict()
        if "txn_amount_paise" in platform.columns
        else {}
    )

    refund_totals_by_original: defaultdict[str, int] = defaultdict(int)
    refunds = platform[
        platform[COL_TXN_TYPE].astype(str).str.upper().eq("REFUND")
        | platform["txn_amount_paise"].fillna(0).astype("Int64").lt(0)
    ].copy()

    for _, refund in refunds.iterrows():
        original_id = refund.get(COL_REFUND_ORIGINAL_TXN_ID)
        if pd.isna(original_id) or not str(original_id).strip():
            continue
        refund_totals_by_original[str(original_id).strip().upper()] += abs(
            int(refund.get("txn_amount_paise", 0) or 0)
        )

    return {
        "original_ids": original_ids,
        "original_amount_map": original_amount_map,
        "refund_totals_by_original": dict(refund_totals_by_original),
    }


def _is_valid_refund(row, refund_context: dict[str, object]) -> bool:
    original_id = row.get(COL_REFUND_ORIGINAL_TXN_ID)
    if pd.isna(original_id) or not str(original_id).strip():
        return False

    original_id = str(original_id).strip().upper()
    original_ids = refund_context.get("original_ids", set())
    original_amount_map = refund_context.get("original_amount_map", {})
    refund_totals_by_original = refund_context.get("refund_totals_by_original", {})

    if original_id not in original_ids:
        return False

    refund_paise = abs(int(row.get("txn_amount_paise", 0) or 0))
    original_amount = int(original_amount_map.get(original_id, 0) or 0)
    cumulative_refund = int(refund_totals_by_original.get(original_id, 0) or 0)
    return refund_paise > 0 and original_amount > 0 and cumulative_refund <= original_amount


def _build_output(merged: pd.DataFrame) -> pd.DataFrame:
    merged["txn_id_final"] = merged[COL_TXN_ID].combine_first(merged[COL_TXN_REF])

    cols = {
        "txn_id_final": "transaction_id",
        COL_TXN_DATE: "txn_date",
        COL_SETTLE_DATE: "settlement_date",
        COL_TXN_AMOUNT: "txn_amount_inr",
        COL_SETTLE_AMOUNT: "settled_amount_inr",
        "txn_amount_paise": "txn_amount_paise",
        "settled_amount_paise": "settled_amount_paise",
        "settlement_count": "settlement_count",
        "delta_inr": "delta_inr",
        "delta_paise": "delta_paise",
        "gap_type": "gap_type",
        "in_platform": "in_platform",
        "in_bank": "in_bank",
        COL_SETTLE_ID: "settlement_id",
        COL_BANK_REF: "bank_reference",
        "is_bank_duplicate": "is_bank_duplicate",
        "is_duplicate_by_ref": "is_duplicate_by_ref",
        "is_duplicate_amount_date": "is_duplicate_amount_date",
        COL_REFUND_ORIGINAL_TXN_ID: COL_REFUND_ORIGINAL_TXN_ID,
    }

    available = {k: v for k, v in cols.items() if k in merged.columns}
    result = merged[list(available.keys())].rename(columns=available)

    gap_order = {
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
    result["_sort"] = result["gap_type"].map(gap_order).fillna(9)
    result = result.sort_values("_sort").drop(columns=["_sort"])
    return result.reset_index(drop=True)


def _append_aggregate_rounding_issue(result: pd.DataFrame, merged: pd.DataFrame) -> pd.DataFrame:
    rounding_rows = merged[merged["gap_type"] == GAP_ROUNDING]
    raw_deltas = rounding_rows.apply(_raw_decimal_delta, axis=1)
    total_raw_delta = sum(raw_deltas, Decimal("0"))

    if abs(total_raw_delta) <= Decimal(str(AGGREGATE_ROUNDING_THRESHOLD)):
        return result

    synthetic = {
        "transaction_id": "AGGREGATE-ROUNDING",
        "txn_date": pd.NaT,
        "settlement_date": pd.NaT,
        "txn_amount_inr": np.nan,
        "settled_amount_inr": np.nan,
        "txn_amount_paise": np.nan,
        "settled_amount_paise": np.nan,
        "settlement_count": np.nan,
        "delta_inr": round(float(total_raw_delta), 5),
        "delta_paise": int((total_raw_delta * PAISE_FACTOR).quantize(Decimal("1"))),
        "gap_type": GAP_AGGREGATE_ROUNDING,
        "in_platform": False,
        "in_bank": False,
        "settlement_id": "",
        "bank_reference": "",
        "is_bank_duplicate": False,
        "is_duplicate_by_ref": False,
        "is_duplicate_amount_date": False,
        COL_REFUND_ORIGINAL_TXN_ID: "",
    }

    return pd.concat([pd.DataFrame([synthetic]), result], ignore_index=True)


def _ensure_money_columns(platform: pd.DataFrame, bank: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    platform = platform.copy()
    bank = bank.copy()

    if "txn_amount_paise" not in platform.columns:
        platform["txn_amount_paise"] = (
            pd.to_numeric(platform[COL_TXN_AMOUNT], errors="coerce") * PAISE_FACTOR
        ).round().astype("Int64")
    if "txn_amount_raw_decimal" not in platform.columns:
        platform["txn_amount_raw_decimal"] = platform[COL_TXN_AMOUNT].apply(lambda v: Decimal(str(v)))

    if "settled_amount_paise" not in bank.columns:
        bank["settled_amount_paise"] = (
            pd.to_numeric(bank[COL_SETTLE_AMOUNT], errors="coerce") * PAISE_FACTOR
        ).round().astype("Int64")
    if "settled_amount_raw_decimal" not in bank.columns:
        bank["settled_amount_raw_decimal"] = bank[COL_SETTLE_AMOUNT].apply(lambda v: Decimal(str(v)))

    return platform, bank


if __name__ == "__main__":
    from src.loader import load_bank_settlements, load_platform_transactions

    platform = load_platform_transactions()
    bank = load_bank_settlements()
    result = reconcile(platform, bank)

    print("\nReconciliation Output (first 10 rows)")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(result.head(10).to_string(index=False))
