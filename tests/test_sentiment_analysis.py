import pandas as pd
from src.sentiment_analysis import add_sentiment_columns, get_sentiment_distribution


def test_add_sentiment_columns_is_generic_not_financial_only():
    df = pd.DataFrame({
        "post_id": ["1", "2", "3"],
        "clean_text": ["good useful product", "bad terrible problem", "ordinary text"],
    })

    result = add_sentiment_columns(df)
    assert result.loc[0, "sentiment_label"] == "positive"
    assert result.loc[1, "sentiment_label"] == "negative"
    assert result.loc[2, "sentiment_label"] == "neutral"

    distribution = get_sentiment_distribution(result)
    assert set(distribution["sentiment_label"]) == {"positive", "negative", "neutral"}
