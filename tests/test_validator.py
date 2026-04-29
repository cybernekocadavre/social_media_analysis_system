import pandas as pd
from src.schema import apply_column_mapping
from src.validator import validate_and_clean_basic, validate_columns


def test_validate_columns_success():
    raw_df = pd.DataFrame({
        "id": [1],
        "author": ["u1"],
        "created_datetime": ["2024-01-01"],
        "title": ["hello"],
        "text": ["world"],
    })

    df = apply_column_mapping(raw_df)
    validate_columns(df)


def test_validate_and_clean_basic_removes_nan_like_required_values():
    df = pd.DataFrame({
        "post_id": ["1", None, "nan", "4"],
        "user_id": ["u1", "u2", "u3", ""],
        "datetime": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "title": ["a", "b", "c", "d"],
        "text": ["text", "text", "text", "text"],
        "likes_count": [1, 2, 3, 4],
        "comments_count": [0, 0, 0, 0],
        "reposts_count": [0, 0, 0, 0],
    })

    result = validate_and_clean_basic(df)
    assert len(result) == 1
    assert result.iloc[0]["post_id"] == "1"
