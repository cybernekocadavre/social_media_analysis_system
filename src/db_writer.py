from typing import Optional
import pandas as pd
from sqlalchemy import text
from src.config import POSTGRES_SCHEMA
from src.db import get_engine

POSTGRES_CHUNKSIZE = 500


def _get_table_columns(table_name: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
        ORDER BY ordinal_position
        """
    )
    result = pd.read_sql(query, engine, params={"schema_name": POSTGRES_SCHEMA, "table_name": table_name})
    return result["column_name"].tolist()


def _align_dataframe_to_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    table_columns = _get_table_columns(table_name)
    if not table_columns:
        return df

    output = df.copy()
    for column in table_columns:
        if column not in output.columns:
            output[column] = None

    return output[[column for column in table_columns if column in output.columns]]


def save_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
) -> None:
    if df is None or df.empty:
        return

    engine = get_engine()
    output_df = _align_dataframe_to_table(df, table_name) if if_exists == "append" else df
    output_df.to_sql(
        name=table_name,
        con=engine,
        schema=POSTGRES_SCHEMA,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=POSTGRES_CHUNKSIZE,
    )


def create_analysis_run_summary(
    total_rows: int,
    total_users: int,
    top_word: str,
    top_source: str,
    source_name: Optional[str] = None,
    nrows_limit: Optional[int] = None,
    top_topic: str = "n/a",
    dominant_sentiment: str = "n/a",
    status: str = "finished",
    error_message: Optional[str] = None,
) -> int:
    engine = get_engine()
    query = text(
        f"""
        INSERT INTO {POSTGRES_SCHEMA}.analysis_runs
        (
            source_name, nrows_limit, status, error_message,
            total_rows, total_users, top_word, top_source, top_topic,
            dominant_sentiment, finished_at
        )
        VALUES
        (
            :source_name, :nrows_limit, :status, :error_message,
            :total_rows, :total_users, :top_word, :top_source, :top_topic,
            :dominant_sentiment, CURRENT_TIMESTAMP
        )
        RETURNING run_id
        """
    )

    with engine.begin() as connection:
        run_id = connection.execute(
            query,
            {
                "source_name": source_name,
                "nrows_limit": nrows_limit,
                "status": status,
                "error_message": error_message,
                "total_rows": int(total_rows),
                "total_users": int(total_users),
                "top_word": str(top_word),
                "top_source": str(top_source),
                "top_topic": str(top_topic),
                "dominant_sentiment": str(dominant_sentiment),
            },
        ).scalar_one()

    return int(run_id)


def attach_run_id(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    output_df = df.copy()
    if "run_id" in output_df.columns:
        output_df["run_id"] = int(run_id)
    else:
        output_df.insert(0, "run_id", int(run_id))
    return output_df


def save_preprocessing_quality_report_to_postgres(
    report_df: pd.DataFrame,
    run_id: int,
) -> None:
    if report_df is None or report_df.empty:
        return

    output_df = report_df.copy()
    output_df.insert(0, "run_id", int(run_id))
    output_df["metric_value"] = output_df["metric_value"].astype(str)

    save_dataframe_to_postgres(output_df, "preprocessing_quality_reports", if_exists="append")


def save_table_dict_to_postgres(tables: dict[str, pd.DataFrame], run_id: int) -> None:
    table_mapping = {
        "top_words": "trend_top_words",
        "top_bigrams": "trend_top_bigrams",
        "top_trigrams": "trend_top_trigrams",
        "tfidf_terms": "trend_tfidf_terms",
        "posts_by_date": "trend_posts_by_date",
        "posts_by_month": "trend_posts_by_month",
        "top_sources": "trend_top_sources",
        "top_companies": "trend_top_companies",
        "top_categories": "trend_top_categories",
        "terms_by_month": "trend_terms_by_month",
        "growing_terms": "trend_growing_terms",
        "sentiment_distribution": "sentiment_distribution",
        "sentiment_by_month": "sentiment_by_month",
        "sentiment_by_source": "sentiment_by_source",
        "topic_terms": "topic_terms",
        "post_topics": "post_topics",
        "topic_summary": "topic_summary",
        "topics_by_month": "topics_by_month",
        "top_users": "user_top_users",
        "top_users_engagement": "user_top_users_engagement",
        "user_distribution": "user_distribution",
        "user_activity_by_month": "user_activity_by_month",
        "engagement_distribution": "user_engagement_distribution",
        "user_features": "user_features",
        "user_segments": "user_segments",
        "user_segment_distribution": "user_segment_distribution",
    }

    for result_key, postgres_table in table_mapping.items():
        save_dataframe_to_postgres(attach_run_id(tables.get(result_key), run_id), postgres_table, if_exists="append")


def save_analysis_result_to_postgres(
    analysis_result: dict,
    source_name: Optional[str] = None,
    nrows_limit: Optional[int] = None,
) -> int:
    summary = analysis_result["summary"]
    run_id = create_analysis_run_summary(
        total_rows=summary["total_rows"],
        total_users=summary["total_users"],
        top_word=summary["top_word"],
        top_source=summary["top_source"],
        source_name=source_name,
        nrows_limit=nrows_limit,
        top_topic=summary.get("top_topic", "n/a"),
        dominant_sentiment=summary.get("dominant_sentiment", "n/a"),
    )

    save_dataframe_to_postgres(attach_run_id(analysis_result["processed_df"], run_id), "processed_posts", if_exists="append")
    save_preprocessing_quality_report_to_postgres(analysis_result["quality_report"], run_id)
    save_table_dict_to_postgres(analysis_result["tables"], run_id)
    return run_id


def save_pipeline_results(
    processed_df: pd.DataFrame,
    preprocessing_quality_report: pd.DataFrame,
    top_words: pd.DataFrame,
    top_bigrams: pd.DataFrame,
    top_trigrams: pd.DataFrame,
    tfidf_terms: pd.DataFrame,
    posts_by_date: pd.DataFrame,
    posts_by_month: pd.DataFrame,
    top_sources: pd.DataFrame,
    top_companies: pd.DataFrame,
    top_categories: pd.DataFrame,
    top_users: pd.DataFrame,
    top_users_engagement: pd.DataFrame,
    user_distribution: pd.DataFrame,
    user_activity_by_month: pd.DataFrame,
    engagement_distribution: pd.DataFrame,
    total_rows: int,
    total_users: int,
    top_word: str,
    top_source: str,
    terms_by_month: Optional[pd.DataFrame] = None,
    growing_terms: Optional[pd.DataFrame] = None,
    sentiment_distribution: Optional[pd.DataFrame] = None,
    sentiment_by_month: Optional[pd.DataFrame] = None,
    sentiment_by_source: Optional[pd.DataFrame] = None,
    topic_terms: Optional[pd.DataFrame] = None,
    post_topics: Optional[pd.DataFrame] = None,
    topic_summary: Optional[pd.DataFrame] = None,
    topics_by_month: Optional[pd.DataFrame] = None,
    user_features: Optional[pd.DataFrame] = None,
    user_segments: Optional[pd.DataFrame] = None,
    user_segment_distribution: Optional[pd.DataFrame] = None,
    source_name: Optional[str] = None,
    nrows_limit: Optional[int] = None,
    top_topic: str = "n/a",
    dominant_sentiment: str = "n/a",
) -> int:
    run_id = create_analysis_run_summary(
        total_rows=total_rows,
        total_users=total_users,
        top_word=top_word,
        top_source=top_source,
        source_name=source_name,
        nrows_limit=nrows_limit,
        top_topic=top_topic,
        dominant_sentiment=dominant_sentiment,
    )

    save_dataframe_to_postgres(attach_run_id(processed_df, run_id), "processed_posts", if_exists="append")
    save_preprocessing_quality_report_to_postgres(preprocessing_quality_report, run_id)

    save_table_dict_to_postgres(
        {
            "top_words": top_words,
            "top_bigrams": top_bigrams,
            "top_trigrams": top_trigrams,
            "tfidf_terms": tfidf_terms,
            "posts_by_date": posts_by_date,
            "posts_by_month": posts_by_month,
            "top_sources": top_sources,
            "top_companies": top_companies,
            "top_categories": top_categories,
            "terms_by_month": terms_by_month,
            "growing_terms": growing_terms,
            "sentiment_distribution": sentiment_distribution,
            "sentiment_by_month": sentiment_by_month,
            "sentiment_by_source": sentiment_by_source,
            "topic_terms": topic_terms,
            "post_topics": post_topics,
            "topic_summary": topic_summary,
            "topics_by_month": topics_by_month,
            "top_users": top_users,
            "top_users_engagement": top_users_engagement,
            "user_distribution": user_distribution,
            "user_activity_by_month": user_activity_by_month,
            "engagement_distribution": engagement_distribution,
            "user_features": user_features,
            "user_segments": user_segments,
            "user_segment_distribution": user_segment_distribution,
        },
        run_id,
    )

    return run_id
