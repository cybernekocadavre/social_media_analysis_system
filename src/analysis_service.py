from typing import Any, Optional
import pandas as pd
from src.data_loader import load_dataset
from src.preprocessor import preprocess_data
from src.reporting import build_preprocessing_quality_report
from src.sentiment_analysis import (
    add_sentiment_columns,
    get_sentiment_by_month,
    get_sentiment_by_source,
    get_sentiment_distribution,
)
from src.topic_modeling import build_topic_tables
from src.trend_analysis import (
    get_posts_by_date,
    get_posts_by_month,
    get_top_bigrams,
    get_top_field_values,
    get_top_tfidf_terms,
    get_top_trigrams,
    get_top_words,
)
from src.trend_growth import get_growing_terms, get_terms_by_month
from src.user_analysis import (
    get_engagement_distribution,
    get_top_users,
    get_top_users_by_engagement,
    get_user_activity_by_month,
    get_user_activity_distribution,
)
from src.user_segmentation import (
    build_user_features,
    get_user_segment_distribution,
    segment_users,
)
from src.validator import validate_and_clean_basic, validate_columns


def run_full_analysis(
    source: Any = None,
    nrows: Optional[int] = None,
    custom_mapping: Optional[dict] = None,
) -> dict:

    if source is None:
        df = load_dataset(custom_mapping=custom_mapping)
    else:
        df = load_dataset(source=source, nrows=nrows, custom_mapping=custom_mapping)

    validate_columns(df)
    df = validate_and_clean_basic(df)
    validation_stats = df.attrs.get("validation_stats", {})

    df = preprocess_data(df)
    df = add_sentiment_columns(df)
    preprocessing_stats = df.attrs.get("preprocessing_stats", {})
    quality_report = build_preprocessing_quality_report(validation_stats, preprocessing_stats)

    topic_tables = build_topic_tables(df)
    user_features = build_user_features(df)
    user_segments = segment_users(df)
    user_segment_distribution = get_user_segment_distribution(user_segments)

    tables: dict[str, pd.DataFrame] = {
        "top_words": get_top_words(df),
        "top_bigrams": get_top_bigrams(df),
        "top_trigrams": get_top_trigrams(df),
        "tfidf_terms": get_top_tfidf_terms(df),
        "posts_by_date": get_posts_by_date(df),
        "posts_by_month": get_posts_by_month(df),
        "top_sources": get_top_field_values(df, "source"),
        "top_companies": get_top_field_values(df, "company"),
        "top_categories": get_top_field_values(df, "category"),
        "terms_by_month": get_terms_by_month(df),
        "growing_terms": get_growing_terms(df),
        "sentiment_distribution": get_sentiment_distribution(df),
        "sentiment_by_month": get_sentiment_by_month(df),
        "sentiment_by_source": get_sentiment_by_source(df),
        "topic_terms": topic_tables["topic_terms"],
        "post_topics": topic_tables["post_topics"],
        "topic_summary": topic_tables["topic_summary"],
        "topics_by_month": topic_tables["topics_by_month"],
        "top_users": get_top_users(df),
        "top_users_engagement": get_top_users_by_engagement(df),
        "user_distribution": get_user_activity_distribution(df),
        "user_activity_by_month": get_user_activity_by_month(df),
        "engagement_distribution": get_engagement_distribution(df),
        "user_features": user_features,
        "user_segments": user_segments,
        "user_segment_distribution": user_segment_distribution,
    }

    top_words = tables["top_words"]
    top_sources = tables["top_sources"]
    topic_summary = tables["topic_summary"]
    sentiment_distribution = tables["sentiment_distribution"]

    summary = {
        "total_rows": int(len(df)),
        "total_users": int(df["user_id"].nunique()) if "user_id" in df.columns else 0,
        "top_word": str(top_words.iloc[0]["word"]) if not top_words.empty else "n/a",
        "top_source": str(top_sources.iloc[0]["source"]) if not top_sources.empty else "n/a",
        "top_topic": str(topic_summary.iloc[0]["topic_label"]) if not topic_summary.empty else "n/a",
        "dominant_sentiment": str(sentiment_distribution.iloc[0]["sentiment_label"]) if not sentiment_distribution.empty else "n/a",
    }

    return {
        "processed_df": df,
        "matched_columns": df.attrs.get("matched_columns", {}),
        "quality_report": quality_report,
        "tables": tables,
        "summary": summary,
        "validation_stats": validation_stats,
        "preprocessing_stats": preprocessing_stats,
    }
