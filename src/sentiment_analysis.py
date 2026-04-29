import pandas as pd

from src.config import (
    SENTIMENT_NEGATIVE_THRESHOLD,
    SENTIMENT_NEGATIVE_WORDS,
    SENTIMENT_POSITIVE_THRESHOLD,
    SENTIMENT_POSITIVE_WORDS,
)


def _count_words(tokens: list[str], dictionary: set[str]) -> int:
    return sum(1 for token in tokens if token in dictionary)


def add_sentiment_columns(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    if output.empty or "clean_text" not in output.columns:
        output["sentiment_score"] = []
        output["sentiment_label"] = []
        output["positive_word_count"] = []
        output["negative_word_count"] = []
        return output

    positive_counts = []
    negative_counts = []
    scores = []
    labels = []

    for text in output["clean_text"].fillna("").astype(str):
        tokens = text.split()
        positive_count = _count_words(tokens, SENTIMENT_POSITIVE_WORDS)
        negative_count = _count_words(tokens, SENTIMENT_NEGATIVE_WORDS)
        denominator = max(positive_count + negative_count, 1)
        score = (positive_count - negative_count) / denominator

        if score > SENTIMENT_POSITIVE_THRESHOLD:
            label = "positive"
        elif score < SENTIMENT_NEGATIVE_THRESHOLD:
            label = "negative"
        else:
            label = "neutral"

        positive_counts.append(positive_count)
        negative_counts.append(negative_count)
        scores.append(score)
        labels.append(label)

    output["positive_word_count"] = positive_counts
    output["negative_word_count"] = negative_counts
    output["sentiment_score"] = scores
    output["sentiment_label"] = labels
    return output


def get_sentiment_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "sentiment_label" not in df.columns:
        return pd.DataFrame(columns=["sentiment_label", "posts_count", "avg_sentiment_score"])

    return (
        df.groupby("sentiment_label")
        .agg(
            posts_count=("post_id", "count"),
            avg_sentiment_score=("sentiment_score", "mean"),
        )
        .reset_index()
        .sort_values("posts_count", ascending=False)
    )


def get_sentiment_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "month" not in df.columns or "sentiment_label" not in df.columns:
        return pd.DataFrame(columns=["month", "sentiment_label", "posts_count", "avg_sentiment_score"])

    return (
        df.groupby(["month", "sentiment_label"])
        .agg(
            posts_count=("post_id", "count"),
            avg_sentiment_score=("sentiment_score", "mean"),
        )
        .reset_index()
        .sort_values(["month", "sentiment_label"])
    )


def get_sentiment_by_source(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "source" not in df.columns or "sentiment_label" not in df.columns:
        return pd.DataFrame(columns=["source", "sentiment_label", "posts_count", "avg_sentiment_score"])

    return (
        df.groupby(["source", "sentiment_label"])
        .agg(
            posts_count=("post_id", "count"),
            avg_sentiment_score=("sentiment_score", "mean"),
        )
        .reset_index()
        .sort_values(["source", "posts_count"], ascending=[True, False])
    )
