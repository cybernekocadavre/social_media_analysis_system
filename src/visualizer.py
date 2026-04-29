import re
import matplotlib.pyplot as plt
import pandas as pd

from src.config import OUTPUT_CHARTS_DIR


def sanitize_label(value) -> str:
    text = str(value)
    # Убирает эмодзи и прочие нестандартные символы, которые часто ломают matplotlib
    text = re.sub(r"[^\w\s\-.,:/()]+", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()

    if text == "":
        text = "unknown"

    return text


def plot_bar(df: pd.DataFrame, x_col: str, y_col: str, title: str, filename: str) -> None:
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return

    plot_df = df.copy()
    plot_df[x_col] = plot_df[x_col].apply(sanitize_label)

    plt.figure(figsize=(10, 6))
    plt.bar(plot_df[x_col].astype(str), plot_df[y_col])
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / filename)
    plt.close()


def plot_line(df: pd.DataFrame, x_col: str, y_col: str, title: str, filename: str) -> None:
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return

    plot_df = df.copy()
    plot_df[x_col] = plot_df[x_col].apply(sanitize_label)

    plt.figure(figsize=(12, 6))
    plt.plot(plot_df[x_col].astype(str), plot_df[y_col])
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / filename)
    plt.close()