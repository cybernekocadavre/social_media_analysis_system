from collections import Counter
import pandas as pd
from src.config import TOP_N_GROWING_TERMS, TOP_N_TREND_TERMS

TERMS_BY_MONTH_COLUMNS = [
    "month",
    "term",
    "count",
    "total_posts_in_month",
    "relative_frequency",
]

GROWING_TERMS_COLUMNS = [
    "term",
    "previous_month",
    "latest_month",
    "previous_count",
    "latest_count",
    "absolute_growth",
    "previous_total_posts",
    "latest_total_posts",
    "previous_relative_frequency",
    "latest_relative_frequency",
    "relative_growth",
    "growth_rate",
    "growth_percent",
    "trend_score",
]


def _empty_terms_by_month() -> pd.DataFrame:
    return pd.DataFrame(columns=TERMS_BY_MONTH_COLUMNS)


def _empty_growing_terms() -> pd.DataFrame:
    return pd.DataFrame(columns=GROWING_TERMS_COLUMNS)


def _global_top_terms(df: pd.DataFrame, top_n_terms: int) -> list[str]:
    if df.empty or "clean_text" not in df.columns:
        return []

    counter = Counter()
    for text in df["clean_text"].fillna("").astype(str):
        counter.update(text.split())

    return [term for term, _ in counter.most_common(top_n_terms)]


def _get_last_two_complete_months(df: pd.DataFrame, available_months: list[str]) -> list[str]:
    months = sorted(str(month) for month in available_months if pd.notna(month))
    if len(months) < 2:
        return []

    if "datetime" not in df.columns:
        return months[-2:]

    dates = pd.to_datetime(df["datetime"], errors="coerce")
    if dates.dropna().empty:
        return months[-2:]

    max_date = dates.max()
    max_month = max_date.to_period("M").strftime("%Y-%m")

    # Если последняя дата датасета не является последним днем месяца,
    # последний месяц считаем неполным и исключаем из сравнения.
    is_last_month_complete = max_date.day == max_date.days_in_month

    if is_last_month_complete:
        complete_months = [month for month in months if month <= max_month]
    else:
        complete_months = [month for month in months if month < max_month]

    if len(complete_months) < 2:
        return []

    return complete_months[-2:]


def get_terms_by_month(df: pd.DataFrame, top_n_terms: int = TOP_N_TREND_TERMS) -> pd.DataFrame:
    if df.empty or "month" not in df.columns or "clean_text" not in df.columns:
        return _empty_terms_by_month()

    top_terms = set(_global_top_terms(df, top_n_terms=top_n_terms))
    if not top_terms:
        return _empty_terms_by_month()

    rows = []
    for month, group in df.groupby("month"):
        total_posts_in_month = int(len(group))
        if total_posts_in_month == 0:
            continue

        counter = Counter()
        for text in group["clean_text"].fillna("").astype(str):
            counter.update(token for token in text.split() if token in top_terms)

        for term, count in counter.items():
            rows.append(
                {
                    "month": str(month),
                    "term": str(term),
                    "count": int(count),
                    "total_posts_in_month": total_posts_in_month,
                    "relative_frequency": float(count / total_posts_in_month),
                }
            )

    if not rows:
        return _empty_terms_by_month()

    return pd.DataFrame(rows).sort_values(
        ["month", "relative_frequency", "count"],
        ascending=[True, False, False],
    )


def get_growing_terms(
    df: pd.DataFrame,
    top_n_terms: int = TOP_N_TREND_TERMS,
    top_n: int = TOP_N_GROWING_TERMS,
) -> pd.DataFrame:

    terms_by_month = get_terms_by_month(df, top_n_terms=top_n_terms)
    if terms_by_month.empty:
        return _empty_growing_terms()

    months = sorted(terms_by_month["month"].dropna().astype(str).unique().tolist())
    selected_months = _get_last_two_complete_months(df, months)
    if len(selected_months) < 2:
        return _empty_growing_terms()

    previous_month, latest_month = selected_months

    count_pivot = terms_by_month.pivot_table(
        index="term",
        columns="month",
        values="count",
        aggfunc="sum",
        fill_value=0,
    )
    relative_pivot = terms_by_month.pivot_table(
        index="term",
        columns="month",
        values="relative_frequency",
        aggfunc="sum",
        fill_value=0.0,
    )

    month_totals = (
        terms_by_month[["month", "total_posts_in_month"]]
        .drop_duplicates()
        .set_index("month")["total_posts_in_month"]
        .to_dict()
    )

    for month in [previous_month, latest_month]:
        if month not in count_pivot.columns:
            count_pivot[month] = 0
        if month not in relative_pivot.columns:
            relative_pivot[month] = 0.0

    result = pd.DataFrame(
        {
            "term": count_pivot.index.astype(str),
            "previous_month": previous_month,
            "latest_month": latest_month,
            "previous_count": count_pivot[previous_month].astype(int).values,
            "latest_count": count_pivot[latest_month].astype(int).values,
            "previous_total_posts": int(month_totals.get(previous_month, 0)),
            "latest_total_posts": int(month_totals.get(latest_month, 0)),
            "previous_relative_frequency": relative_pivot[previous_month].astype(float).values,
            "latest_relative_frequency": relative_pivot[latest_month].astype(float).values,
        }
    )

    result["absolute_growth"] = result["latest_count"] - result["previous_count"]
    result["relative_growth"] = (
        result["latest_relative_frequency"] - result["previous_relative_frequency"]
    )

    denominator = result["previous_relative_frequency"].where(
        result["previous_relative_frequency"] > 0,
        1.0,
    )
    result["growth_rate"] = result["relative_growth"] / denominator
    result["growth_percent"] = result["growth_rate"] * 100
    result["trend_score"] = result["relative_growth"] * result["latest_count"]

    result = result[result["relative_growth"] > 0].copy()
    if result.empty:
        return _empty_growing_terms()

    return (
        result[GROWING_TERMS_COLUMNS]
        .sort_values(["relative_growth", "latest_count", "absolute_growth"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
