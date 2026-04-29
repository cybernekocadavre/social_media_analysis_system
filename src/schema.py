import re
from typing import Optional
import pandas as pd
from src.config import (
    AUTO_DETECT_COLUMNS,
    COLUMN_ALIASES,
    COLUMN_MAPPING,
    OPTIONAL_COLUMNS_DEFAULTS,
    REQUIRED_CANONICAL_COLUMNS,
    TEXT_SOURCE_COLUMNS,
)


def normalize_column_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", "_", name)
    return name.strip("_")


def apply_column_mapping(
    df: pd.DataFrame,
    custom_mapping: Optional[dict] = None,
) -> pd.DataFrame:
    df = df.copy()

    original_columns = list(df.columns)

    normalized_map = {col: normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=normalized_map)

    mapping_to_use = custom_mapping if custom_mapping is not None else COLUMN_MAPPING
    matched_columns = {}

    for raw_name, canonical_name in mapping_to_use.items():
        normalized_raw = normalize_column_name(raw_name)
        if normalized_raw in df.columns and canonical_name not in matched_columns:
            matched_columns[canonical_name] = normalized_raw

    if AUTO_DETECT_COLUMNS:
        for canonical_name, aliases in COLUMN_ALIASES.items():
            if canonical_name in matched_columns:
                continue

            normalized_aliases = [normalize_column_name(alias) for alias in aliases]
            normalized_aliases.append(normalize_column_name(canonical_name))

            for alias in normalized_aliases:
                if alias in df.columns:
                    matched_columns[canonical_name] = alias
                    break

    rename_dict = {source_name: canonical_name for canonical_name, source_name in matched_columns.items()}
    df = df.rename(columns=rename_dict)

    real_text_columns = [col for col in TEXT_SOURCE_COLUMNS if col in matched_columns]

    df.attrs["original_columns"] = original_columns
    df.attrs["matched_columns"] = matched_columns
    df.attrs["real_text_columns"] = real_text_columns

    for col, default_value in OPTIONAL_COLUMNS_DEFAULTS.items():
        if col not in df.columns:
            df[col] = default_value

    return df


def validate_canonical_schema(df: pd.DataFrame) -> None:
    missing_required = [col for col in REQUIRED_CANONICAL_COLUMNS if col not in df.columns]
    if missing_required:
        raise ValueError(f"Отсутствуют обязательные канонические поля: {missing_required}")

    real_text_columns = df.attrs.get("real_text_columns", [])
    if not real_text_columns:
        raise ValueError(
            "Не найдено ни одного реального текстового поля для анализа. "
            "Проверь COLUMN_MAPPING или ручное сопоставление колонок."
        )
