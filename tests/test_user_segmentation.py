import pandas as pd
from src.user_segmentation import build_user_features, get_user_segment_distribution, segment_users


def test_segment_users_returns_segments():
    df = pd.DataFrame({
        "post_id": ["1", "2", "3", "4", "5", "6"],
        "user_id": ["u1", "u1", "u1", "u2", "u3", "u3"],
        "likes_count": [10, 12, 8, 1, 50, 60],
        "comments_count": [1, 2, 1, 0, 10, 12],
        "reposts_count": [0, 0, 0, 0, 1, 1],
        "engagement_score": [11, 14, 9, 1, 61, 73],
        "source": ["a", "a", "b", "a", "b", "b"],
        "category": ["x", "x", "y", "x", "z", "z"],
        "month": ["2024-01", "2024-01", "2024-02", "2024-01", "2024-02", "2024-02"],
    })

    features = build_user_features(df)
    assert not features.empty

    segments = segment_users(df)
    assert "segment_label" in segments.columns
    assert "segment_id" in segments.columns

    distribution = get_user_segment_distribution(segments)
    assert not distribution.empty
