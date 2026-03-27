import os
import sys

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import (
    GAP_AGGREGATE_ROUNDING,
    GAP_CROSS_MONTH,
    GAP_DELAYED_SETTLEMENT,
    GAP_DUPLICATE,
    GAP_MATCHED,
    GAP_ORPHAN_REFUND,
    GAP_PARTIAL_SETTLEMENT,
    GAP_ROUNDING,
    GAP_UNSETTLED,
)
from src.reconciler import reconcile


def make_platform(rows: list) -> pd.DataFrame:
    if not rows:
        rows = [
            {
                "transaction_id": "",
                "customer_name": "",
                "transaction_date": "2024-01-01",
                "amount_inr": 0.0,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    df = pd.DataFrame(rows)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["amount_inr"] = pd.to_numeric(df["amount_inr"], errors="coerce")
    df["status"] = df.get("status", "SUCCESS")
    df["type"] = df.get("type", "CREDIT")
    if "original_transaction_id" not in df.columns:
        df["original_transaction_id"] = pd.NA
    return df


def make_bank(rows: list) -> pd.DataFrame:
    if not rows:
        rows = [
            {
                "settlement_id": "",
                "transaction_ref": "",
                "settlement_date": "2024-01-01",
                "settled_amount_inr": 0.0,
                "bank_reference": "",
            }
        ]
    df = pd.DataFrame(rows)
    df["settlement_date"] = pd.to_datetime(df["settlement_date"])
    df["settled_amount_inr"] = pd.to_numeric(df["settled_amount_inr"], errors="coerce")
    return df


def get_gap(result: pd.DataFrame, txn_id: str) -> str | None:
    rows = result[result["transaction_id"] == txn_id]
    if rows.empty:
        return None
    return rows.iloc[0]["gap_type"]


def test_clean_match():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-01",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 5000.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-01",
                "transaction_ref": "TXN-TEST-01",
                "settlement_date": "2024-01-11",
                "settled_amount_inr": 5000.00,
                "bank_reference": "HDFC-0001",
            }
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-01") == GAP_MATCHED
    assert result[result["transaction_id"] == "TXN-TEST-01"]["delta_inr"].iloc[0] == 0.0


def test_cross_month():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-02",
                "customer_name": "Test User",
                "transaction_date": "2024-01-31",
                "amount_inr": 9875.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-02",
                "transaction_ref": "TXN-TEST-02",
                "settlement_date": "2024-02-01",
                "settled_amount_inr": 9875.00,
                "bank_reference": "HDFC-0002",
            }
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-02") == GAP_CROSS_MONTH


def test_rounding_diff():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-03",
                "customer_name": "Test User",
                "transaction_date": "2024-01-11",
                "amount_inr": 99.995,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-03",
                "transaction_ref": "TXN-TEST-03",
                "settlement_date": "2024-01-12",
                "settled_amount_inr": 100.00,
                "bank_reference": "ICICI-0003",
            }
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-03") == GAP_ROUNDING
    delta = result[result["transaction_id"] == "TXN-TEST-03"]["delta_inr"].iloc[0]
    assert abs(delta - 0.005) < 0.0001


def test_rounding_does_not_fire_for_real_mismatch():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-03B",
                "customer_name": "Test User",
                "transaction_date": "2024-01-11",
                "amount_inr": 1000.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-03B",
                "transaction_ref": "TXN-TEST-03B",
                "settlement_date": "2024-01-12",
                "settled_amount_inr": 1500.00,
                "bank_reference": "ICICI-0003B",
            }
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-03B") != GAP_ROUNDING


def test_duplicate_settlement_same_reference():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-04",
                "customer_name": "Test User",
                "transaction_date": "2024-01-28",
                "amount_inr": 3200.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-04A",
                "transaction_ref": "TXN-TEST-04",
                "settlement_date": "2024-01-29",
                "settled_amount_inr": 3200.00,
                "bank_reference": "KOTAK-0004A",
            },
            {
                "settlement_id": "SET-TEST-04B",
                "transaction_ref": "TXN-TEST-04",
                "settlement_date": "2024-01-29",
                "settled_amount_inr": 3200.00,
                "bank_reference": "KOTAK-0004B",
            },
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-04") == GAP_DUPLICATE


def test_duplicate_amount_date_different_refs():
    platform = make_platform([])
    platform = platform[platform["transaction_id"] == "NO_MATCH"].reset_index(drop=True)
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-04C",
                "transaction_ref": "BANK-REF-A",
                "settlement_date": "2024-01-29",
                "settled_amount_inr": 777.00,
                "bank_reference": "KOTAK-1001",
            },
            {
                "settlement_id": "SET-TEST-04D",
                "transaction_ref": "BANK-REF-B",
                "settlement_date": "2024-01-29",
                "settled_amount_inr": 777.00,
                "bank_reference": "KOTAK-1002",
            },
        ]
    )
    result = reconcile(platform, bank)
    flagged = result[result["gap_type"] == GAP_DUPLICATE]["transaction_id"].tolist()
    assert set(flagged) == {"BANK-REF-A", "BANK-REF-B"}


def test_orphan_refund():
    platform = make_platform(
        [
            {
                "transaction_id": "REF-TEST-05",
                "customer_name": "Test User",
                "transaction_date": "2024-01-15",
                "amount_inr": -250.00,
                "status": "SUCCESS",
                "type": "REFUND",
            }
        ]
    )
    bank = make_bank([])
    bank = bank[bank["transaction_ref"] == "NO_MATCH"].reset_index(drop=True)
    result = reconcile(platform, bank)
    assert get_gap(result, "REF-TEST-05") == GAP_ORPHAN_REFUND


def test_delayed_settlement():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-06",
                "customer_name": "Test User",
                "transaction_date": "2024-01-01",
                "amount_inr": 1000.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-06",
                "transaction_ref": "TXN-TEST-06",
                "settlement_date": "2024-01-10",
                "settled_amount_inr": 1000.00,
                "bank_reference": "HDFC-0006",
            }
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-06") == GAP_DELAYED_SETTLEMENT


def test_partial_settlement():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-07",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 1000.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            }
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-07A",
                "transaction_ref": "TXN-TEST-07",
                "settlement_date": "2024-01-11",
                "settled_amount_inr": 600.00,
                "bank_reference": "AXIS-0007A",
            },
            {
                "settlement_id": "SET-TEST-07B",
                "transaction_ref": "TXN-TEST-07",
                "settlement_date": "2024-01-12",
                "settled_amount_inr": 400.00,
                "bank_reference": "AXIS-0007B",
            },
        ]
    )
    result = reconcile(platform, bank)
    assert get_gap(result, "TXN-TEST-07") == GAP_PARTIAL_SETTLEMENT


def test_refund_linked_to_original_is_not_orphan():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-08",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 500.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            },
            {
                "transaction_id": "REF-TEST-08",
                "customer_name": "Test User",
                "transaction_date": "2024-01-12",
                "amount_inr": -200.00,
                "status": "SUCCESS",
                "type": "REFUND",
                "original_transaction_id": "TXN-TEST-08",
            },
        ]
    )
    bank = make_bank([])
    bank = bank[bank["transaction_ref"] == "NO_MATCH"].reset_index(drop=True)
    result = reconcile(platform, bank)
    assert get_gap(result, "REF-TEST-08") == GAP_UNSETTLED


def test_refund_exceeding_original_is_orphan_refund():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-09",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 500.00,
                "status": "SUCCESS",
                "type": "CREDIT",
            },
            {
                "transaction_id": "REF-TEST-09A",
                "customer_name": "Test User",
                "transaction_date": "2024-01-12",
                "amount_inr": -300.00,
                "status": "SUCCESS",
                "type": "REFUND",
                "original_transaction_id": "TXN-TEST-09",
            },
            {
                "transaction_id": "REF-TEST-09B",
                "customer_name": "Test User",
                "transaction_date": "2024-01-13",
                "amount_inr": -250.00,
                "status": "SUCCESS",
                "type": "REFUND",
                "original_transaction_id": "TXN-TEST-09",
            },
        ]
    )
    bank = make_bank([])
    bank = bank[bank["transaction_ref"] == "NO_MATCH"].reset_index(drop=True)
    result = reconcile(platform, bank)
    assert get_gap(result, "REF-TEST-09A") == GAP_ORPHAN_REFUND
    assert get_gap(result, "REF-TEST-09B") == GAP_ORPHAN_REFUND


def test_aggregate_rounding_issue():
    platform = make_platform(
        [
            {
                "transaction_id": "TXN-TEST-10A",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 99.994,
                "status": "SUCCESS",
                "type": "CREDIT",
            },
            {
                "transaction_id": "TXN-TEST-10B",
                "customer_name": "Test User",
                "transaction_date": "2024-01-10",
                "amount_inr": 49.994,
                "status": "SUCCESS",
                "type": "CREDIT",
            },
        ]
    )
    bank = make_bank(
        [
            {
                "settlement_id": "SET-TEST-10A",
                "transaction_ref": "TXN-TEST-10A",
                "settlement_date": "2024-01-11",
                "settled_amount_inr": 100.00,
                "bank_reference": "HDFC-1010A",
            },
            {
                "settlement_id": "SET-TEST-10B",
                "transaction_ref": "TXN-TEST-10B",
                "settlement_date": "2024-01-11",
                "settled_amount_inr": 50.00,
                "bank_reference": "HDFC-1010B",
            },
        ]
    )
    result = reconcile(platform, bank)
    assert GAP_AGGREGATE_ROUNDING in set(result["gap_type"])
    assert get_gap(result, "TXN-TEST-10A") == GAP_ROUNDING
    assert get_gap(result, "TXN-TEST-10B") == GAP_ROUNDING


def test_full_dataset():
    from src.loader import load_bank_settlements, load_platform_transactions

    platform = load_platform_transactions()
    bank = load_bank_settlements()
    result = reconcile(platform, bank)
    gaps_found = set(result[result["gap_type"] != GAP_MATCHED]["gap_type"].unique())

    assert GAP_CROSS_MONTH in gaps_found
    assert GAP_ROUNDING in gaps_found
    assert GAP_DUPLICATE in gaps_found
    assert GAP_ORPHAN_REFUND in gaps_found
    assert len(result) >= 19
