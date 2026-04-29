import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from src.config import RANDOM_STATE, USER_SEGMENT_CLUSTERS


def build_user_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "user_id", "posts_count", "avg_likes", "avg_comments", "total_comments",
                "total_engagement", "avg_engagement", "unique_sources", "unique_categories",
                "active_months"
            ]
        )

    aggregations = {
        "posts_count": ("post_id", "count"),
        "avg_likes": ("likes_count", "mean"),
        "avg_comments": ("comments_count", "mean"),
        "total_comments": ("comments_count", "sum"),
        "total_engagement": ("engagement_score", "sum"),
        "avg_engagement": ("engagement_score", "mean"),
    }

    if "source" in df.columns:
        aggregations["unique_sources"] = ("source", "nunique")
    if "category" in df.columns:
        aggregations["unique_categories"] = ("category", "nunique")
    if "month" in df.columns:
        aggregations["active_months"] = ("month", "nunique")

    result = df.groupby("user_id").agg(**aggregations).reset_index()

    for optional_col in ["unique_sources", "unique_categories", "active_months"]:
        if optional_col not in result.columns:
            result[optional_col] = 0

    numeric_cols = [col for col in result.columns if col != "user_id"]
    result[numeric_cols] = result[numeric_cols].fillna(0)
    return result


def _activity_label(row: pd.Series) -> str:
    posts_count = row.get("posts_count", 0)
    avg_engagement = row.get("avg_engagement", 0)

    if posts_count <= 1 and avg_engagement <= 5:
        return "low_activity"
    if posts_count >= 5 or avg_engagement >= 20:
        return "high_activity"
    return "medium_activity"


def segment_users(df: pd.DataFrame, n_clusters: int = USER_SEGMENT_CLUSTERS) -> pd.DataFrame:
    features = build_user_features(df)
    if features.empty:
        return pd.DataFrame(
            columns=list(features.columns) + ["segment_id", "segment_label"]
        )

    result = features.copy()
    result["segment_label"] = result.apply(_activity_label, axis=1)

    numeric_columns = [
        "posts_count", "avg_likes", "avg_comments", "total_comments",
        "total_engagement", "avg_engagement", "unique_sources", "unique_categories",
        "active_months"
    ]
    numeric_columns = [col for col in numeric_columns if col in result.columns]

    if len(result) < 2 or len(numeric_columns) == 0:
        result["segment_id"] = 0
        return result

    actual_clusters = max(1, min(n_clusters, len(result)))
    scaled_features = StandardScaler().fit_transform(result[numeric_columns])

    model = KMeans(n_clusters=actual_clusters, random_state=RANDOM_STATE, n_init=10)
    result["segment_id"] = model.fit_predict(scaled_features).astype(int)

    return result


def get_user_segment_distribution(user_segments: pd.DataFrame) -> pd.DataFrame:
    if user_segments.empty or "segment_label" not in user_segments.columns:
        return pd.DataFrame(columns=["segment_label", "users_count", "avg_posts_count", "avg_engagement"])

    return (
        user_segments.groupby("segment_label")
        .agg(
            users_count=("user_id", "count"),
            avg_posts_count=("posts_count", "mean"),
            avg_engagement=("avg_engagement", "mean"),
        )
        .reset_index()
        .sort_values("users_count", ascending=False)
    )
