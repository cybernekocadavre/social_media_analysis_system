import pandas as pd
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer
from src.config import N_TOPICS, RANDOM_STATE, TOPIC_MODEL_MAX_FEATURES, TOP_N_TOPIC_TERMS


def _empty_topic_tables() -> dict[str, pd.DataFrame]:
    return {
        "topic_terms": pd.DataFrame(columns=["topic_id", "topic_label", "term", "weight", "term_rank"]),
        "post_topics": pd.DataFrame(columns=["post_id", "topic_id", "topic_label", "topic_weight"]),
        "topic_summary": pd.DataFrame(columns=["topic_id", "topic_label", "posts_count", "avg_topic_weight"]),
        "topics_by_month": pd.DataFrame(columns=["month", "topic_id", "topic_label", "posts_count", "avg_topic_weight"]),
    }


def build_topic_tables(
    df: pd.DataFrame,
    n_topics: int = N_TOPICS,
    top_n_terms: int = TOP_N_TOPIC_TERMS,
    max_features: int = TOPIC_MODEL_MAX_FEATURES,
) -> dict[str, pd.DataFrame]:

    if df.empty or "clean_text" not in df.columns or "post_id" not in df.columns:
        return _empty_topic_tables()

    working_df = df[["post_id", "clean_text"] + (["month"] if "month" in df.columns else [])].copy()
    working_df["clean_text"] = working_df["clean_text"].fillna("").astype(str).str.strip()
    working_df = working_df.loc[working_df["clean_text"].ne("")]

    if len(working_df) < 2:
        return _empty_topic_tables()

    vectorizer = TfidfVectorizer(max_features=max_features, min_df=1)

    try:
        matrix = vectorizer.fit_transform(working_df["clean_text"])
    except ValueError:
        return _empty_topic_tables()

    feature_names = vectorizer.get_feature_names_out()
    if matrix.shape[1] < 2:
        return _empty_topic_tables()

    actual_topics = max(1, min(n_topics, matrix.shape[0], matrix.shape[1]))

    model = NMF(
        n_components=actual_topics,
        random_state=RANDOM_STATE,
        init="nndsvda" if actual_topics <= min(matrix.shape) else "random",
        max_iter=500,
    )

    try:
        document_topic_matrix = model.fit_transform(matrix)
    except ValueError:
        return _empty_topic_tables()

    topic_rows = []
    topic_labels = {}

    for topic_id, topic_weights in enumerate(model.components_):
        top_indices = topic_weights.argsort()[::-1][:top_n_terms]
        label_terms = [feature_names[index] for index in top_indices[:3]]
        topic_label = ", ".join(label_terms) if label_terms else f"topic_{topic_id}"
        topic_labels[topic_id] = topic_label

        for rank, index in enumerate(top_indices, start=1):
            topic_rows.append({
                "topic_id": int(topic_id),
                "topic_label": topic_label,
                "term": str(feature_names[index]),
                "weight": float(topic_weights[index]),
                "term_rank": int(rank),
            })

    dominant_topic_ids = document_topic_matrix.argmax(axis=1)
    dominant_topic_weights = document_topic_matrix.max(axis=1)

    post_topics = pd.DataFrame({
        "post_id": working_df["post_id"].astype(str).values,
        "topic_id": dominant_topic_ids.astype(int),
        "topic_weight": dominant_topic_weights.astype(float),
    })
    post_topics["topic_label"] = post_topics["topic_id"].map(topic_labels)
    post_topics = post_topics[["post_id", "topic_id", "topic_label", "topic_weight"]]

    topic_summary = (
        post_topics.groupby(["topic_id", "topic_label"])
        .agg(
            posts_count=("post_id", "count"),
            avg_topic_weight=("topic_weight", "mean"),
        )
        .reset_index()
        .sort_values("posts_count", ascending=False)
    )

    if "month" in working_df.columns:
        topic_with_month = post_topics.merge(
            working_df[["post_id", "month"]].astype({"post_id": str}),
            on="post_id",
            how="left",
        )
        topics_by_month = (
            topic_with_month.groupby(["month", "topic_id", "topic_label"])
            .agg(
                posts_count=("post_id", "count"),
                avg_topic_weight=("topic_weight", "mean"),
            )
            .reset_index()
            .sort_values(["month", "posts_count"], ascending=[True, False])
        )
    else:
        topics_by_month = pd.DataFrame(columns=["month", "topic_id", "topic_label", "posts_count", "avg_topic_weight"])

    return {
        "topic_terms": pd.DataFrame(topic_rows),
        "post_topics": post_topics,
        "topic_summary": topic_summary,
        "topics_by_month": topics_by_month,
    }
