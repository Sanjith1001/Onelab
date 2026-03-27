import logging
import os
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd

try:
    from pydantic import BaseModel, ConfigDict, ValidationError
except ImportError:  # pragma: no cover
    BaseModel = None
    ConfigDict = None
    ValidationError = Exception

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import (
    BANK_FILE,
    COL_REFUND_ORIGINAL_TXN_ID,
    COL_SETTLE_AMOUNT,
    COL_SETTLE_DATE,
    COL_SETTLE_ID,
    COL_TXN_AMOUNT,
    COL_TXN_DATE,
    COL_TXN_ID,
    COL_TXN_REF,
    COL_TXN_STATUS,
    COL_TXN_TYPE,
    PAISE_FACTOR,
    PLATFORM_FILE,
)


logger = logging.getLogger("payrecon.loader")


if BaseModel is not None:
    class PlatformRow(BaseModel):
        model_config = ConfigDict(extra="ignore")
        transaction_id: str
        transaction_date: object
        amount_inr: Decimal
        status: str
        type: str


    class BankRow(BaseModel):
        model_config = ConfigDict(extra="ignore")
        settlement_id: str
        transaction_ref: str
        settlement_date: object
        settled_amount_inr: Decimal
else:
    PlatformRow = None
    BankRow = None


def load_platform_transactions() -> pd.DataFrame:
    logger.info("Reading platform file: %s", PLATFORM_FILE)
    if not os.path.exists(PLATFORM_FILE):
        raise FileNotFoundError(f"Platform file not found: {PLATFORM_FILE}")

    df = pd.read_csv(PLATFORM_FILE)
    required = [COL_TXN_ID, COL_TXN_DATE, COL_TXN_AMOUNT, COL_TXN_STATUS, COL_TXN_TYPE]
    _check_columns(df, required, "platform_transactions")

    df[COL_TXN_ID] = df[COL_TXN_ID].astype(str).str.strip().str.upper()
    df[COL_TXN_DATE] = pd.to_datetime(df[COL_TXN_DATE], format="%Y-%m-%d", errors="coerce")
    df[COL_TXN_STATUS] = df[COL_TXN_STATUS].astype(str).str.strip().str.upper()
    df[COL_TXN_TYPE] = df[COL_TXN_TYPE].astype(str).str.strip().str.upper()

    if COL_REFUND_ORIGINAL_TXN_ID not in df.columns:
        df[COL_REFUND_ORIGINAL_TXN_ID] = pd.NA
    df[COL_REFUND_ORIGINAL_TXN_ID] = (
        df[COL_REFUND_ORIGINAL_TXN_ID].astype(str).str.strip().str.upper().replace({"": pd.NA, "NAN": pd.NA})
    )

    df["_txn_amount_decimal"] = df[COL_TXN_AMOUNT].apply(lambda v: _to_decimal(v, COL_TXN_AMOUNT))
    df["txn_amount_raw_decimal"] = df["_txn_amount_decimal"]
    _validate_rows(df, PlatformRow, "platform_transactions", required)

    df["txn_amount_paise"] = df["_txn_amount_decimal"].apply(_to_paise)
    df[COL_TXN_AMOUNT] = df["txn_amount_paise"] / PAISE_FACTOR

    null_amount = df[COL_TXN_AMOUNT].isnull().sum()
    if null_amount > 0:
        logger.warning("%s rows have null amount - dropping them", null_amount)
        df = df.dropna(subset=[COL_TXN_AMOUNT])

    df = df.drop(columns=["_txn_amount_decimal"])
    logger.info("%s platform transactions loaded", len(df))
    return df.reset_index(drop=True)


def load_bank_settlements() -> pd.DataFrame:
    logger.info("Reading bank file: %s", BANK_FILE)
    if not os.path.exists(BANK_FILE):
        raise FileNotFoundError(f"Bank file not found: {BANK_FILE}")

    df = pd.read_csv(BANK_FILE)
    required = [COL_SETTLE_ID, COL_TXN_REF, COL_SETTLE_DATE, COL_SETTLE_AMOUNT]
    _check_columns(df, required, "bank_settlements")

    df[COL_SETTLE_ID] = df[COL_SETTLE_ID].astype(str).str.strip().str.upper()
    df[COL_TXN_REF] = df[COL_TXN_REF].astype(str).str.strip().str.upper()
    df[COL_SETTLE_DATE] = pd.to_datetime(df[COL_SETTLE_DATE], format="%Y-%m-%d", errors="coerce")
    df["_settled_amount_decimal"] = df[COL_SETTLE_AMOUNT].apply(lambda v: _to_decimal(v, COL_SETTLE_AMOUNT))
    df["settled_amount_raw_decimal"] = df["_settled_amount_decimal"]
    _validate_rows(df, BankRow, "bank_settlements", required)

    df["settled_amount_paise"] = df["_settled_amount_decimal"].apply(_to_paise)
    df[COL_SETTLE_AMOUNT] = df["settled_amount_paise"] / PAISE_FACTOR

    null_amount = df[COL_SETTLE_AMOUNT].isnull().sum()
    if null_amount > 0:
        logger.warning("%s rows have null settled amount - dropping them", null_amount)
        df = df.dropna(subset=[COL_SETTLE_AMOUNT])

    df = df.drop(columns=["_settled_amount_decimal"])
    logger.info("%s bank settlement records loaded", len(df))
    return df.reset_index(drop=True)


def _check_columns(df: pd.DataFrame, required: list[str], file_label: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"[loader] Missing columns in {file_label}: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )


def _to_decimal(value, field_name: str) -> Decimal:
    if pd.isna(value):
        return Decimal("NaN")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid numeric value in {field_name}: {value}") from exc


def _to_paise(amount: Decimal) -> int:
    quantized = (amount * PAISE_FACTOR).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(quantized)


def _validate_rows(df: pd.DataFrame, model, label: str, required: list[str]):
    if model is None:
        _validate_rows_without_pydantic(df, label, required)
        return

    errors = []
    for idx, record in enumerate(df.to_dict(orient="records"), start=2):
        try:
            model.model_validate(record)
        except ValidationError as exc:
            errors.append(f"row {idx}: {exc.errors()[0]['msg']}")
            if len(errors) >= 5:
                break

    if errors:
        raise ValueError(f"[loader] Schema validation failed for {label}: {'; '.join(errors)}")


def _validate_rows_without_pydantic(df: pd.DataFrame, label: str, required: list[str]):
    errors = []
    for idx, record in enumerate(df.to_dict(orient="records"), start=2):
        for column in required:
            value = record.get(column)
            if pd.isna(value) or (isinstance(value, str) and not value.strip()):
                errors.append(f"row {idx}: {column} is required")
                break
        if len(errors) >= 5:
            break

    if errors:
        raise ValueError(f"[loader] Schema validation failed for {label}: {'; '.join(errors)}")


def preview(df: pd.DataFrame, label: str, n: int = 5):
    print(f"\n{label} (first {n} rows)")
    print(df.head(n).to_string(index=False))
    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} cols\n")


if __name__ == "__main__":
    platform = load_platform_transactions()
    bank = load_bank_settlements()
    preview(platform, "Platform Transactions")
    preview(bank, "Bank Settlements")
