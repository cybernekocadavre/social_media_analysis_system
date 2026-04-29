import pandas as pd
from src.config import (
    OUTPUT_REPORTS_DIR,
    PREPROCESSING_QUALITY_CSV_FILENAME,
    PREPROCESSING_QUALITY_TXT_FILENAME,
)


def save_text_summary(
    total_rows: int,
    total_users: int,
    top_word: str,
    top_source: str,
) -> None:
    report_path = OUTPUT_REPORTS_DIR / "summary_report.txt"

    lines = [
        "Краткая сводка по результатам анализа",
        f"Количество записей: {total_rows}",
        f"Количество уникальных пользователей: {total_users}",
        f"Наиболее частотное слово: {top_word}",
        f"Наиболее частый источник/категория: {top_source}",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")


def build_preprocessing_quality_report(validation_stats: dict, preprocessing_stats: dict) -> pd.DataFrame:
    merged_stats = {}
    merged_stats.update(validation_stats or {})
    merged_stats.update(preprocessing_stats or {})

    rows = [
        {"metric_name": key, "metric_value": value}
        for key, value in merged_stats.items()
    ]
    return pd.DataFrame(rows)


def save_preprocessing_quality_report(
    validation_stats: dict,
    preprocessing_stats: dict,
) -> pd.DataFrame:
    report_df = build_preprocessing_quality_report(validation_stats, preprocessing_stats)

    txt_path = OUTPUT_REPORTS_DIR / PREPROCESSING_QUALITY_TXT_FILENAME
    csv_path = OUTPUT_REPORTS_DIR / PREPROCESSING_QUALITY_CSV_FILENAME

    if not report_df.empty:
        report_df.to_csv(csv_path, index=False)

        lines = ["Журнал качества предобработки данных"]
        for _, row in report_df.iterrows():
            lines.append(f"{row['metric_name']}: {row['metric_value']}")
        txt_path.write_text("\n".join(lines), encoding="utf-8")

    return report_df
