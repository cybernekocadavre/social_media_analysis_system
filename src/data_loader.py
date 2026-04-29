from pathlib import Path
from typing import Any, Optional
import pandas as pd
from src.config import (
    AUTO_DETECT_CSV_SEPARATOR,
    CSV_DECIMAL,
    CSV_ENCODINGS_TO_TRY,
    CSV_LOW_MEMORY,
    CSV_QUOTECHAR,
    CSV_SEPARATOR,
    EXCEL_SHEET_NAME,
    JSON_ORIENT,
    PROCESSED_DIR,
    RAW_DATA_PATH,
    SAVE_INTERMEDIATE_FILES,
    TEST_ROWS,
)
from src.schema import apply_column_mapping


def _get_source_name(source: Any) -> str:
    if isinstance(source, (str, Path)):
        return Path(source).name
    if hasattr(source, "name"):
        return str(source.name)
    return "uploaded_file.csv"


def _reset_source_if_possible(source: Any) -> None:
    if hasattr(source, "seek"):
        try:
            source.seek(0)
        except Exception:
            pass


def _read_csv_with_fallbacks(source: Any, nrows: Optional[int] = TEST_ROWS) -> pd.DataFrame:
    last_error = None

    separators_to_try = [CSV_SEPARATOR]
    if AUTO_DETECT_CSV_SEPARATOR:
        separators_to_try = [None, CSV_SEPARATOR, ",", ";", "\t"]

    cleaned_separators = []
    for sep in separators_to_try:
        if sep not in cleaned_separators:
            cleaned_separators.append(sep)

    for encoding in CSV_ENCODINGS_TO_TRY:
        for sep in cleaned_separators:
            try:
                _reset_source_if_possible(source)
                kwargs = {
                    "nrows": nrows,
                    "encoding": encoding,
                    "decimal": CSV_DECIMAL,
                    "quotechar": CSV_QUOTECHAR,
                    "low_memory": CSV_LOW_MEMORY,
                }

                if sep is None:
                    kwargs["sep"] = None
                    kwargs["engine"] = "python"
                else:
                    kwargs["sep"] = sep

                return pd.read_csv(source, **kwargs)
            except Exception as error:
                last_error = error

    raise ValueError(f"Не удалось прочитать CSV-файл. Последняя ошибка: {last_error}")


def read_raw_dataset(
    source: Any = RAW_DATA_PATH,
    nrows: Optional[int] = TEST_ROWS,
) -> pd.DataFrame:
    _reset_source_if_possible(source)

    source_name = _get_source_name(source)
    suffix = Path(source_name).suffix.lower()

    if suffix == ".csv":
        df = _read_csv_with_fallbacks(source=source, nrows=nrows)
    elif suffix in [".xlsx", ".xls"]:
        _reset_source_if_possible(source)
        df = pd.read_excel(
            source,
            sheet_name=EXCEL_SHEET_NAME,
            nrows=nrows,
        )
    elif suffix == ".json":
        _reset_source_if_possible(source)
        json_kwargs = {}
        if JSON_ORIENT is not None:
            json_kwargs["orient"] = JSON_ORIENT

        df = pd.read_json(source, **json_kwargs)
        if nrows is not None:
            df = df.head(nrows)
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {suffix}")

    return df


def load_dataset(
    source: Any = RAW_DATA_PATH,
    nrows: Optional[int] = TEST_ROWS,
    custom_mapping: Optional[dict] = None,
) -> pd.DataFrame:
    df = read_raw_dataset(source=source, nrows=nrows)

    if SAVE_INTERMEDIATE_FILES:
        raw_preview_path = PROCESSED_DIR / f"first_{nrows or 'all'}_raw.csv"
        df.to_csv(raw_preview_path, index=False)

    df = apply_column_mapping(df, custom_mapping=custom_mapping)

    if SAVE_INTERMEDIATE_FILES:
        mapped_preview_path = PROCESSED_DIR / f"first_{nrows or 'all'}_mapped.csv"
        df.to_csv(mapped_preview_path, index=False)

    return df
