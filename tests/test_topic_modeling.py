import pandas as pd
from src.topic_modeling import build_topic_tables


def test_build_topic_tables_returns_topic_summary():
    df = pd.DataFrame({
        "post_id": ["1", "2", "3", "4"],
        "month": ["2024-01", "2024-01", "2024-02", "2024-02"],
        "clean_text": [
            "cat animal pet kitten",
            "dog animal pet puppy",
            "python code data model",
            "python data analysis code",
        ],
    })

    result = build_topic_tables(df, n_topics=2, top_n_terms=3)
    assert not result["topic_terms"].empty
    assert not result["post_topics"].empty
    assert not result["topic_summary"].empty
    assert not result["topics_by_month"].empty
