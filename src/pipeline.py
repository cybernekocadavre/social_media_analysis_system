from pathlib import Path
from typing import Any, Optional
from src.analysis_service import run_full_analysis
from src.config import OUTPUT_TABLES_DIR, USE_PLOTLY, USE_POSTGRES
from src.db_init import initialize_postgres
from src.db_writer import save_analysis_result_to_postgres
from src.plotly_visualizer import make_bar_figure, make_line_figure, save_plotly_figure
from src.reporting import save_preprocessing_quality_report, save_text_summary
from src.visualizer import plot_bar, plot_line


def _source_name(source: Any) -> str:
    if source is None:
        return "config.py RAW_DATA_PATH"
    if isinstance(source, (str, Path)):
        return Path(source).name
    if hasattr(source, "name"):
        return str(source.name)
    return "unknown_source"


def _save_tables(tables: dict) -> None:
    for table_name, table_df in tables.items():
        if table_df is not None:
            table_df.to_csv(OUTPUT_TABLES_DIR / f"{table_name}.csv", index=False)


def _save_static_charts(tables: dict) -> None:
    plot_bar(tables["top_words"], "word", "count", "Top words", "top_words.png")
    plot_bar(tables["top_bigrams"], "bigram", "count", "Top bigrams", "top_bigrams.png")
    plot_bar(tables["top_trigrams"], "trigram", "count", "Top trigrams", "top_trigrams.png")
    plot_line(tables["posts_by_date"], "date", "post_count", "Posts by date", "posts_by_date.png")
    plot_line(tables["posts_by_month"], "month", "post_count", "Posts by month", "posts_by_month.png")
    plot_bar(tables["top_sources"], "source", "count", "Top sources", "top_sources.png")
    plot_bar(tables["top_categories"], "category", "count", "Top categories", "top_categories.png")
    plot_bar(tables["growing_terms"], "term", "absolute_growth", "Growing terms", "growing_terms.png")
    plot_bar(tables["sentiment_distribution"], "sentiment_label", "posts_count", "Sentiment distribution", "sentiment_distribution.png")
    plot_bar(tables["topic_summary"], "topic_label", "posts_count", "Topic summary", "topic_summary.png")
    plot_bar(tables["top_users"], "user_id", "posts_count", "Top active users", "top_users.png")
    plot_bar(tables["user_distribution"], "activity_group", "users_count", "User activity distribution", "user_distribution.png")
    plot_bar(tables["engagement_distribution"], "engagement_group", "posts_count", "Engagement distribution", "engagement_distribution.png")
    plot_bar(tables["user_segment_distribution"], "segment_label", "users_count", "User segment distribution", "user_segment_distribution.png")
    plot_line(tables["user_activity_by_month"], "month", "total_posts", "User activity by month", "user_activity_by_month.png")


def _save_plotly_charts(tables: dict) -> None:
    figure_specs = [
        (make_bar_figure(tables["top_words"], "word", "count", "Top words"), "plotly_top_words"),
        (make_line_figure(tables["posts_by_date"], "date", "post_count", "Posts by date"), "plotly_posts_by_date"),
        (make_line_figure(tables["posts_by_month"], "month", "post_count", "Posts by month"), "plotly_posts_by_month"),
        (make_bar_figure(tables["top_sources"], "source", "count", "Top sources"), "plotly_top_sources"),
        (make_bar_figure(tables["top_categories"], "category", "count", "Top categories"), "plotly_top_categories"),
        (make_bar_figure(tables["growing_terms"], "term", "absolute_growth", "Growing terms"), "plotly_growing_terms"),
        (make_bar_figure(tables["sentiment_distribution"], "sentiment_label", "posts_count", "Sentiment distribution"), "plotly_sentiment_distribution"),
        (make_bar_figure(tables["topic_summary"], "topic_label", "posts_count", "Topic summary"), "plotly_topic_summary"),
        (make_bar_figure(tables["top_users"], "user_id", "posts_count", "Top active users"), "plotly_top_users"),
        (make_bar_figure(tables["user_distribution"], "activity_group", "users_count", "User activity distribution"), "plotly_user_distribution"),
        (make_bar_figure(tables["engagement_distribution"], "engagement_group", "posts_count", "Engagement distribution"), "plotly_engagement_distribution"),
        (make_line_figure(tables["user_activity_by_month"], "month", "total_posts", "User activity by month"), "plotly_user_activity_by_month"),
        (make_bar_figure(tables["user_segment_distribution"], "segment_label", "users_count", "User segment distribution"), "plotly_user_segment_distribution"),
    ]

    for fig, filename in figure_specs:
        if fig is not None:
            save_plotly_figure(fig, filename)


def run_pipeline(source: Any = None, nrows: Optional[int] = None, custom_mapping: Optional[dict] = None) -> None:
    print("1. Загрузка, валидация, предобработка и полный анализ...")
    analysis_result = run_full_analysis(source=source, nrows=nrows, custom_mapping=custom_mapping)
    tables = analysis_result["tables"]
    summary = analysis_result["summary"]

    print("2. Сохранение таблиц...")
    _save_tables(tables)

    print("3. Построение статических графиков...")
    _save_static_charts(tables)

    if USE_PLOTLY:
        print("4. Построение интерактивных графиков Plotly...")
        _save_plotly_charts(tables)

    print("5. Сохранение краткой сводки и журнала качества...")
    save_text_summary(
        total_rows=summary["total_rows"],
        total_users=summary["total_users"],
        top_word=summary["top_word"],
        top_source=summary["top_source"],
    )
    save_preprocessing_quality_report(
        validation_stats=analysis_result["validation_stats"],
        preprocessing_stats=analysis_result["preprocessing_stats"],
    )

    if USE_POSTGRES:
        print("6. Сохранение результатов в PostgreSQL...")
        initialize_postgres()
        run_id = save_analysis_result_to_postgres(
            analysis_result,
            source_name=_source_name(source),
            nrows_limit=nrows,
        )
        print(f"Результаты сохранены в PostgreSQL, run_id={run_id}")

    print("Готово. Все результаты сохранены.")
