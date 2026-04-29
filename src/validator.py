import pandas as pd
from src.config import (
    DROP_ROWS_WITH_EMPTY_TEXT,
    NUMERIC_COLUMNS,
    PROCESSED_DIR,
    SAVE_INTERMEDIATE_FILES,
)
from src.schema import validate_canonical_schema

INVALID_REQUIRED_VALUES = {"", "nan", "none", "null", "nat", "unknown"}


def validate_columns(df: pd.DataFrame) -> None:
    validate_canonical_schema(df)


def _is_valid_required_value(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    return series.notna() & ~cleaned.isin(INVALID_REQUIRED_VALUES)


def validate_and_clean_basic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    input_rows = len(df)

    before_required_drop = len(df)
    required_mask = (
        _is_valid_required_value(df["post_id"])
        & _is_valid_required_value(df["user_id"])
        & df["datetime"].notna()
        & ~df["datetime"].astype(str).str.strip().str.lower().isin(INVALID_REQUIRED_VALUES)
    )
    df = df.loc[required_mask].copy()
    missing_required_removed = before_required_drop - len(df)

    df["post_id"] = df["post_id"].astype(str).str.strip()
    df["user_id"] = df["user_id"].astype(str).str.strip()

    before_duplicates = len(df)
    df = df.drop_duplicates(subset=["post_id"])
    duplicates_removed = before_duplicates - len(df)

    for text_col in ["title", "text"]:
        if text_col in df.columns:
            df[text_col] = df[text_col].fillna("").astype(str)

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    empty_text_removed = 0
    if DROP_ROWS_WITH_EMPTY_TEXT:
        title_series = df["title"] if "title" in df.columns else pd.Series("", index=df.index)
        text_series = df["text"] if "text" in df.columns else pd.Series("", index=df.index)
        raw_text = (title_series.astype(str) + " " + text_series.astype(str)).str.strip()
        before_empty_text = len(df)
        df = df.loc[raw_text.ne("")]
        empty_text_removed = before_empty_text - len(df)

    validation_stats = {
        "loaded_rows": int(input_rows),
        "missing_required_removed": int(missing_required_removed),
        "duplicates_removed": int(duplicates_removed),
        "empty_text_removed_at_validation": int(empty_text_removed),
        "rows_after_validation": int(len(df)),
    }
    df.attrs["validation_stats"] = validation_stats

    if SAVE_INTERMEDIATE_FILES:
        validated_path = PROCESSED_DIR / "validated_data.csv"
        df.to_csv(validated_path, index=False)

    return df
