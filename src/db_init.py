from sqlalchemy import text
from src.config import POSTGRES_SCHEMA
from src.db import get_engine


def _table_name(name: str) -> str:
    return f"{POSTGRES_SCHEMA}.{name}"


def _create_fk_statement(table_name: str) -> str:
    constraint_name = f"fk_{table_name}_run_id"
    return f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = '{constraint_name}'
        ) THEN
            ALTER TABLE {POSTGRES_SCHEMA}.{table_name}
            ADD CONSTRAINT {constraint_name}
            FOREIGN KEY (run_id)
            REFERENCES {POSTGRES_SCHEMA}.analysis_runs(run_id)
            ON DELETE CASCADE
            NOT VALID;
        END IF;
    END $$;
    """


def initialize_postgres() -> None:
    engine = get_engine()

    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};",
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('analysis_runs')} (
            run_id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            source_name TEXT,
            nrows_limit BIGINT,
            status TEXT DEFAULT 'finished',
            error_message TEXT,
            total_rows BIGINT,
            total_users BIGINT,
            top_word TEXT,
            top_source TEXT,
            top_topic TEXT,
            dominant_sentiment TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('preprocessing_quality_reports')} (
            run_id INTEGER,
            metric_name TEXT,
            metric_value TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('processed_posts')} (
            run_id INTEGER,
            post_id TEXT,
            title TEXT,
            text TEXT,
            created_utc DOUBLE PRECISION,
            datetime TIMESTAMP,
            user_id TEXT,
            likes_count DOUBLE PRECISION,
            comments_count DOUBLE PRECISION,
            upvote_ratio DOUBLE PRECISION,
            category TEXT,
            permalink TEXT,
            url TEXT,
            source TEXT,
            company TEXT,
            reposts_count DOUBLE PRECISION,
            full_text TEXT,
            clean_text TEXT,
            date DATE,
            month TEXT,
            engagement_score DOUBLE PRECISION,
            text_length INTEGER,
            positive_word_count INTEGER,
            negative_word_count INTEGER,
            sentiment_score DOUBLE PRECISION,
            sentiment_label TEXT
        );
        """,
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_words')} (run_id INTEGER, word TEXT, count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_bigrams')} (run_id INTEGER, bigram TEXT, count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_trigrams')} (run_id INTEGER, trigram TEXT, count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_tfidf_terms')} (run_id INTEGER, term TEXT, score DOUBLE PRECISION);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_posts_by_date')} (run_id INTEGER, date DATE, post_count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_posts_by_month')} (run_id INTEGER, month TEXT, post_count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_sources')} (run_id INTEGER, source TEXT, count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_companies')} (run_id INTEGER, company TEXT, count BIGINT);",
        f"CREATE TABLE IF NOT EXISTS {_table_name('trend_top_categories')} (run_id INTEGER, category TEXT, count BIGINT);",
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('trend_terms_by_month')} (
            run_id INTEGER,
            month TEXT,
            term TEXT,
            count BIGINT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('trend_growing_terms')} (
            run_id INTEGER,
            term TEXT,
            previous_month TEXT,
            latest_month TEXT,
            previous_count BIGINT,
            latest_count BIGINT,
            absolute_growth BIGINT,
            growth_rate DOUBLE PRECISION,
            trend_score DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('sentiment_distribution')} (
            run_id INTEGER,
            sentiment_label TEXT,
            posts_count BIGINT,
            avg_sentiment_score DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('sentiment_by_month')} (
            run_id INTEGER,
            month TEXT,
            sentiment_label TEXT,
            posts_count BIGINT,
            avg_sentiment_score DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('sentiment_by_source')} (
            run_id INTEGER,
            source TEXT,
            sentiment_label TEXT,
            posts_count BIGINT,
            avg_sentiment_score DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('topic_terms')} (
            run_id INTEGER,
            topic_id INTEGER,
            topic_label TEXT,
            term TEXT,
            weight DOUBLE PRECISION,
            term_rank INTEGER
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('post_topics')} (
            run_id INTEGER,
            post_id TEXT,
            topic_id INTEGER,
            topic_label TEXT,
            topic_weight DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('topic_summary')} (
            run_id INTEGER,
            topic_id INTEGER,
            topic_label TEXT,
            posts_count BIGINT,
            avg_topic_weight DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('topics_by_month')} (
            run_id INTEGER,
            month TEXT,
            topic_id INTEGER,
            topic_label TEXT,
            posts_count BIGINT,
            avg_topic_weight DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_top_users')} (
            run_id INTEGER,
            user_id TEXT,
            posts_count BIGINT,
            avg_likes DOUBLE PRECISION,
            total_comments DOUBLE PRECISION,
            total_engagement DOUBLE PRECISION,
            avg_engagement DOUBLE PRECISION
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_top_users_engagement')} (
            run_id INTEGER,
            user_id TEXT,
            posts_count BIGINT,
            total_engagement DOUBLE PRECISION,
            avg_engagement DOUBLE PRECISION
        );
        """,
        f"CREATE TABLE IF NOT EXISTS {_table_name('user_distribution')} (run_id INTEGER, activity_group TEXT, users_count BIGINT);",
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_activity_by_month')} (
            run_id INTEGER,
            month TEXT,
            unique_users BIGINT,
            total_posts BIGINT,
            avg_engagement DOUBLE PRECISION
        );
        """,
        f"CREATE TABLE IF NOT EXISTS {_table_name('user_engagement_distribution')} (run_id INTEGER, engagement_group TEXT, posts_count BIGINT);",
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_features')} (
            run_id INTEGER,
            user_id TEXT,
            posts_count BIGINT,
            avg_likes DOUBLE PRECISION,
            avg_comments DOUBLE PRECISION,
            total_comments DOUBLE PRECISION,
            total_engagement DOUBLE PRECISION,
            avg_engagement DOUBLE PRECISION,
            unique_sources BIGINT,
            unique_categories BIGINT,
            active_months BIGINT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_segments')} (
            run_id INTEGER,
            user_id TEXT,
            posts_count BIGINT,
            avg_likes DOUBLE PRECISION,
            avg_comments DOUBLE PRECISION,
            total_comments DOUBLE PRECISION,
            total_engagement DOUBLE PRECISION,
            avg_engagement DOUBLE PRECISION,
            unique_sources BIGINT,
            unique_categories BIGINT,
            active_months BIGINT,
            segment_label TEXT,
            segment_id INTEGER
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name('user_segment_distribution')} (
            run_id INTEGER,
            segment_label TEXT,
            users_count BIGINT,
            avg_posts_count DOUBLE PRECISION,
            avg_engagement DOUBLE PRECISION
        );
        """,
    ]

    alter_statements = [
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS source_name TEXT;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS nrows_limit BIGINT;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'finished';",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS error_message TEXT;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS top_topic TEXT;",
        f"ALTER TABLE {_table_name('analysis_runs')} ADD COLUMN IF NOT EXISTS dominant_sentiment TEXT;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS positive_word_count INTEGER;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS negative_word_count INTEGER;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS sentiment_score DOUBLE PRECISION;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS sentiment_label TEXT;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS created_utc DOUBLE PRECISION;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS upvote_ratio DOUBLE PRECISION;",
        f"ALTER TABLE {_table_name('processed_posts')} ADD COLUMN IF NOT EXISTS permalink TEXT;",
    ]

    run_id_tables = [
        "preprocessing_quality_reports",
        "processed_posts",
        "trend_top_words",
        "trend_top_bigrams",
        "trend_top_trigrams",
        "trend_tfidf_terms",
        "trend_posts_by_date",
        "trend_posts_by_month",
        "trend_top_sources",
        "trend_top_companies",
        "trend_top_categories",
        "trend_terms_by_month",
        "trend_growing_terms",
        "sentiment_distribution",
        "sentiment_by_month",
        "sentiment_by_source",
        "topic_terms",
        "post_topics",
        "topic_summary",
        "topics_by_month",
        "user_top_users",
        "user_top_users_engagement",
        "user_distribution",
        "user_activity_by_month",
        "user_engagement_distribution",
        "user_features",
        "user_segments",
        "user_segment_distribution",
    ]

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))

        for statement in alter_statements:
            connection.execute(text(statement))

        for table_name in run_id_tables:
            connection.execute(
                text(
                    f"ALTER TABLE IF EXISTS {_table_name(table_name)} "
                    f"ADD COLUMN IF NOT EXISTS run_id INTEGER"
                )
            )

        for table_name in run_id_tables:
            connection.execute(text(_create_fk_statement(table_name)))
            connection.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_run_id "
                    f"ON {_table_name(table_name)} (run_id);"
                )
            )

        index_statements = [
            f"CREATE INDEX IF NOT EXISTS idx_processed_posts_post_id ON {_table_name('processed_posts')} (post_id);",
            f"CREATE INDEX IF NOT EXISTS idx_processed_posts_user_id ON {_table_name('processed_posts')} (user_id);",
            f"CREATE INDEX IF NOT EXISTS idx_processed_posts_datetime ON {_table_name('processed_posts')} (datetime);",
            f"CREATE INDEX IF NOT EXISTS idx_processed_posts_month ON {_table_name('processed_posts')} (month);",
            f"CREATE INDEX IF NOT EXISTS idx_processed_posts_sentiment_label ON {_table_name('processed_posts')} (sentiment_label);",
            f"CREATE INDEX IF NOT EXISTS idx_post_topics_post_id ON {_table_name('post_topics')} (post_id);",
        ]
        for statement in index_statements:
            connection.execute(text(statement))
