import pandas as pd
from src.trend_analysis import get_top_words, get_top_bigrams


def test_get_top_words():
    df = pd.DataFrame({"clean_text": ["apple stock growth", "apple market growth"]})
    result = get_top_words(df, top_n=3)
    assert not result.empty
    assert "apple" in result["word"].values


def test_get_top_bigrams():
    df = pd.DataFrame({"clean_text": ["apple stock growth", "apple stock market"]})
    result = get_top_bigrams(df, top_n=3)
    assert not result.empty