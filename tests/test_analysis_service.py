from pathlib import Path
import pandas as pd

from src.analysis_service import run_full_analysis


def test_run_full_analysis_e2e_small_csv(tmp_path: Path):
    csv_path = tmp_path / "sample.csv"
    pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "author": ["u1", "u1", "u2", "u3"],
        "created_datetime": ["2024-01-01", "2024-01-02", "2024-02-01", "2024-02-02"],
        "title": ["Good launch", "Bad issue", "Useful guide", "Ordinary note"],
        "text": ["success improve", "problem failed", "good useful", "plain text"],
        "score": [10, 1, 5, 0],
        "num_comments": [2, 3, 1, 0],
        "subreddit": ["test", "test", "demo", "demo"],
    }).to_csv(csv_path, index=False)

    result = run_full_analysis(source=csv_path, nrows=None)
    assert result["summary"]["total_rows"] == 4
    assert "growing_terms" in result["tables"]
    assert "topic_summary" in result["tables"]
    assert "sentiment_distribution" in result["tables"]
    assert "user_segments" in result["tables"]
