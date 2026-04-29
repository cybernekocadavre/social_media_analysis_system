import pandas as pd
from src.user_analysis import get_top_users


def test_get_top_users():
    df = pd.DataFrame({
        "post_id": [1, 2, 3],
        "user_id": ["u1", "u1", "u2"],
        "likes_count": [10, 5, 3],
        "comments_count": [2, 1, 1],
        "reposts_count": [0, 0, 0],
        "engagement_score": [12, 6, 4],
    })
    result = get_top_users(df, top_n=2)
    assert result.iloc[0]["user_id"] == "u1"