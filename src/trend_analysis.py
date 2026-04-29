from collections import Counter
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from src.config import (
    TOP_N_BIGRAMS,
    TOP_N_FIELDS,
    TOP_N_TRIGRAMS,
    TOP_N_WORDS,
)


def _get_non_empty_texts(df: pd.DataFrame) -> list[str]:
    if "clean_text" not in df.columns:
        return []

    texts = df["clean_text"].fillna("").astype(str)
    texts = texts[texts.str.strip().ne("")]
    return texts.tolist()


def get_top_words(df: pd.DataFrame, top_n: int = TOP_N_WORDS) -> pd.DataFrame:
    texts = _get_non_empty_texts(df)
    if not texts:
        return pd.DataFrame(columns=["word", "count"])

    all_words = " ".join(texts).split()
    counter = Counter(all_words)
    return pd.DataFrame(counter.most_common(top_n), columns=["word", "count"])


def get_top_ngrams(df: pd.DataFrame, n: int = 2, top_n: int = 20) -> pd.DataFrame:
    texts = _get_non_empty_texts(df)
    if not texts:
        column_name = "bigram" if n == 2 else "trigram" if n == 3 else "ngram"
        return pd.DataFrame(columns=[column_name, "count"])

    ngrams = []
    for text in texts:
        words = text.split()
        if len(words) >= n:
            ngrams.extend([" ".join(words[i:i+n]) for i in range(len(words) - n + 1)])

    counter = Counter(ngrams)
    column_name = "bigram" if n == 2 else "trigram" if n == 3 else "ngram"
    return pd.DataFrame(counter.most_common(top_n), columns=[column_name, "count"])


def get_top_bigrams(df: pd.DataFrame, top_n: int = TOP_N_BIGRAMS) -> pd.DataFrame:
    return get_top_ngrams(df, n=2, top_n=top_n)


def get_top_trigrams(df: pd.DataFrame, top_n: int = TOP_N_TRIGRAMS) -> pd.DataFrame:
    return get_top_ngrams(df, n=3, top_n=top_n)


def get_top_tfidf_terms(df: pd.DataFrame, top_n: int = TOP_N_WORDS) -> pd.DataFrame:
    texts = _get_non_empty_texts(df)
    if not texts:
        return pd.DataFrame(columns=["term", "score"])

    vectorizer = TfidfVectorizer(max_features=1000)

    try:
        matrix = vectorizer.fit_transform(texts)
    except ValueError:
        return pd.DataFrame(columns=["term", "score"])

    scores = matrix.mean(axis=0).A1
    terms = vectorizer.get_feature_names_out()

    return (
        pd.DataFrame({"term": terms, "score": scores})
        .sort_values("score", ascending=False)
        .head(top_n)
    )


def get_posts_by_date(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns or df.empty:
        return pd.DataFrame(columns=["date", "post_count"])

    return (
        df.groupby("date")
        .size()
        .reset_index(name="post_count")
        .sort_values("date")
    )


def get_posts_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if "month" not in df.columns or df.empty:
        return pd.DataFrame(columns=["month", "post_count"])

    return (
        df.groupby("month")
        .size()
        .reset_index(name="post_count")
        .sort_values("month")
    )


def get_top_field_values(df: pd.DataFrame, field_name: str, top_n: int = TOP_N_FIELDS) -> pd.DataFrame:
    if field_name not in df.columns or df.empty:
        return pd.DataFrame(columns=[field_name, "count"])

    data = df[field_name].fillna("unknown").astype(str)
    result = (
        data.to_frame(name=field_name)
        .groupby(field_name)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(top_n)
    )
    return result
