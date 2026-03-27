import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

PLATFORM_FILE = os.path.join(DATA_DIR, "platform_transactions.csv")
BANK_FILE = os.path.join(DATA_DIR, "bank_settlements.csv")

REPORT_FILE = os.path.join(OUTPUT_DIR, "reconciliation_report.csv")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "summary_report.csv")
MISMATCHES_FILE = os.path.join(OUTPUT_DIR, "mismatches_only.csv")

COL_TXN_ID = "transaction_id"
COL_CUSTOMER = "customer_name"
COL_TXN_DATE = "transaction_date"
COL_TXN_AMOUNT = "amount_inr"
COL_TXN_STATUS = "status"
COL_TXN_TYPE = "type"
COL_REFUND_ORIGINAL_TXN_ID = "original_transaction_id"

COL_SETTLE_ID = "settlement_id"
COL_TXN_REF = "transaction_ref"
COL_SETTLE_DATE = "settlement_date"
COL_SETTLE_AMOUNT = "settled_amount_inr"
COL_BANK_REF = "bank_reference"

ROUNDING_EPSILON = 0.01
SETTLEMENT_WINDOW_DAYS = 2
AGGREGATE_ROUNDING_THRESHOLD = 0.01
PAISE_FACTOR = 100
CURRENCY_CODE = "INR"
CURRENCY_SYMBOL = "Rs "
RECON_PERIOD_MONTH = 1
RECON_PERIOD_YEAR = 2024
DATASET_NOTE = "Synthetic test data generated for the reconciliation assessment."
ASSESSIONS_ASSUMPTIONS = [
    "Platform transactions are recorded instantly on the transaction date.",
    "Bank settlements arrive one to two calendar days later unless a delayed settlement is being simulated.",
    "A valid reconciliation normally has one bank settlement per platform transaction reference.",
    "Amounts are compared in integer paise and raw decimal drift is used only for rounding diagnostics.",
    "Refunds must reference a valid original transaction and cannot exceed the original amount in aggregate.",
    "Cross-month settlement is treated as a reporting gap even when the amount matches exactly.",
]

GAP_MATCHED = "MATCHED"
GAP_CROSS_MONTH = "CROSS_MONTH"
GAP_ROUNDING = "ROUNDING_DIFF"
GAP_DUPLICATE = "DUPLICATE"
GAP_ORPHAN_REFUND = "ORPHAN_REFUND"
GAP_UNMATCHED = "UNMATCHED"
GAP_UNSETTLED = "UNSETTLED"
GAP_DELAYED_SETTLEMENT = "DELAYED_SETTLEMENT"
GAP_PARTIAL_SETTLEMENT = "PARTIAL_SETTLEMENT"
GAP_AGGREGATE_ROUNDING = "AGGREGATE_ROUNDING_ISSUE"
