import pandas as pd
from src.trend_growth import get_growing_terms, get_terms_by_month


def test_get_growing_terms_uses_monthly_dynamics():
    df = pd.DataFrame({
        "month": ["2024-01", "2024-01", "2024-02", "2024-02", "2024-02"],
        "clean_text": ["apple banana", "banana", "apple apple", "apple", "cherry"],
    })

    by_month = get_terms_by_month(df, top_n_terms=3)
    assert not by_month.empty

    growing = get_growing_terms(df, top_n_terms=3, top_n=3)
    assert not growing.empty
    assert "apple" in growing["term"].values
