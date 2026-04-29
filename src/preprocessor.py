import pandas as pd

from src.config import (
    DATETIME_DAYFIRST,
    DATETIME_FORMAT,
    DATETIME_UNIT,
    DATETIME_UTC,
    DROP_ROWS_WITH_EMPTY_TEXT,
    PROCESSED_DIR,
    SAVE_INTERMEDIATE_FILES,
)
from src.text_preprocessing import normalize_text


def parse_datetime_series(series: pd.Series) -> pd.Series:
    series = series.astype(str).str.strip().str.strip('"').str.strip("'")

    if DATETIME_UNIT is not None:
        numeric_series = pd.to_numeric(series, errors="coerce")
        return pd.to_datetime(
            numeric_series,
            errors="coerce",
            unit=DATETIME_UNIT,
            utc=DATETIME_UTC,
        )

    parsed = pd.to_datetime(
        series,
        errors="coerce",
        format=DATETIME_FORMAT,
        dayfirst=DATETIME_DAYFIRST,
        utc=DATETIME_UTC,
    )

    if parsed.notna().sum() == 0:
        numeric_series = pd.to_numeric(series, errors="coerce")

        if numeric_series.notna().sum() > 0:
            parsed_seconds = pd.to_datetime(
                numeric_series,
                errors="coerce",
                unit="s",
                utc=DATETIME_UTC,
            )
            parsed_milliseconds = pd.to_datetime(
                numeric_series,
                errors="coerce",
                unit="ms",
                utc=DATETIME_UTC,
            )

            parsed = (
                parsed_seconds
                if parsed_seconds.notna().sum() >= parsed_milliseconds.notna().sum()
                else parsed_milliseconds
            )

    return parsed


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    input_rows = len(df)
    validation_stats = df.attrs.get("validation_stats", {})

    df["title"] = df["title"].fillna("").astype(str)
    df["text"] = df["text"].fillna("").astype(str)
    df["user_id"] = df["user_id"].fillna("unknown").astype(str)

    df["full_text"] = (df["title"] + " " + df["text"]).str.strip()
    df["clean_text"] = df["full_text"].apply(normalize_text)

    empty_text_removed_preprocessing = 0
    if DROP_ROWS_WITH_EMPTY_TEXT:
        before_text_filter = len(df)
        df = df.loc[df["clean_text"].astype(str).str.strip().ne("")]
        empty_text_removed_preprocessing = before_text_filter - len(df)

    before_datetime_filter = len(df)
    df["datetime"] = parse_datetime_series(df["datetime"])
    df = df.dropna(subset=["datetime"])
    invalid_datetime_removed = before_datetime_filter - len(df)

    df["date"] = df["datetime"].dt.date
    df["month"] = df["datetime"].dt.to_period("M").astype(str)

    for col in ["likes_count", "comments_count", "reposts_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["engagement_score"] = (
        df["likes_count"] + df["comments_count"] + df["reposts_count"]
    )
    df["text_length"] = df["clean_text"].apply(lambda x: len(str(x).split()))

    preprocessing_stats = {
        "rows_before_preprocessing": int(input_rows),
        "empty_text_removed_at_preprocessing": int(empty_text_removed_preprocessing),
        "invalid_datetime_removed": int(invalid_datetime_removed),
        "final_clean_rows": int(len(df)),
    }

    merged_stats = {}
    merged_stats.update(validation_stats)
    merged_stats.update(preprocessing_stats)
    df.attrs["validation_stats"] = validation_stats
    df.attrs["preprocessing_stats"] = preprocessing_stats
    df.attrs["quality_report_stats"] = merged_stats

    if SAVE_INTERMEDIATE_FILES:
        cleaned_path = PROCESSED_DIR / "cleaned_data.csv"
        df.to_csv(cleaned_path, index=False)

    return df
