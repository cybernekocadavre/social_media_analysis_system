import pandas as pd
from src.config import ENGAGEMENT_BINS, ENGAGEMENT_LABELS, TOP_N_USERS


def get_top_users(df: pd.DataFrame, top_n: int = TOP_N_USERS) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "user_id",
                "posts_count",
                "avg_likes",
                "total_comments",
                "total_engagement",
                "avg_engagement",
            ]
        )

    result = (
        df.groupby("user_id")
        .agg(
            posts_count=("post_id", "count"),
            avg_likes=("likes_count", "mean"),
            total_comments=("comments_count", "sum"),
            total_engagement=("engagement_score", "sum"),
            avg_engagement=("engagement_score", "mean"),
        )
        .reset_index()
        .sort_values("posts_count", ascending=False)
        .head(top_n)
    )
    return result


def get_top_users_by_engagement(df: pd.DataFrame, top_n: int = TOP_N_USERS) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "user_id",
                "posts_count",
                "total_engagement",
                "avg_engagement",
            ]
        )

    result = (
        df.groupby("user_id")
        .agg(
            posts_count=("post_id", "count"),
            total_engagement=("engagement_score", "sum"),
            avg_engagement=("engagement_score", "mean"),
        )
        .reset_index()
        .sort_values("total_engagement", ascending=False)
        .head(top_n)
    )
    return result


def get_user_activity_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["activity_group", "users_count"])

    user_posts = df.groupby("user_id").size().reset_index(name="posts_count")

    def category(posts: int) -> str:
        if posts == 1:
            return "1 post"
        if 2 <= posts <= 5:
            return "2-5 posts"
        if 6 <= posts <= 10:
            return "6-10 posts"
        return "10+ posts"

    user_posts["activity_group"] = user_posts["posts_count"].apply(category)

    return (
        user_posts.groupby("activity_group")
        .size()
        .reset_index(name="users_count")
    )


def get_user_activity_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if "month" not in df.columns or df.empty:
        return pd.DataFrame(columns=["month", "unique_users", "total_posts", "avg_engagement"])

    return (
        df.groupby("month")
        .agg(
            unique_users=("user_id", "nunique"),
            total_posts=("post_id", "count"),
            avg_engagement=("engagement_score", "mean"),
        )
        .reset_index()
        .sort_values("month")
    )


def get_engagement_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "engagement_score" not in df.columns:
        return pd.DataFrame(columns=["engagement_group", "posts_count"])

    working_df = df.copy()
    working_df["engagement_group"] = pd.cut(
        working_df["engagement_score"],
        bins=ENGAGEMENT_BINS,
        labels=ENGAGEMENT_LABELS,
        include_lowest=True,
    )

    distribution = (
        working_df.groupby("engagement_group", observed=False)
        .size()
        .reset_index(name="posts_count")
    )

    distribution["engagement_group"] = distribution["engagement_group"].astype(str)
    return distribution
