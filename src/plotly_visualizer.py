import plotly.express as px
from src.config import OUTPUT_CHARTS_DIR, SAVE_PLOTLY_HTML, SAVE_PLOTLY_JSON


def save_plotly_figure(fig, filename_base: str) -> None:
    html_path = OUTPUT_CHARTS_DIR / f"{filename_base}.html"
    json_path = OUTPUT_CHARTS_DIR / f"{filename_base}.json"

    if SAVE_PLOTLY_HTML:
        fig.write_html(str(html_path))

    if SAVE_PLOTLY_JSON:
        json_path.write_text(fig.to_json(), encoding="utf-8")


def make_bar_figure(df, x_col: str, y_col: str, title: str):
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return None

    fig = px.bar(df, x=x_col, y=y_col, title=title)
    fig.update_layout(xaxis_title=x_col, yaxis_title=y_col)
    return fig


def make_line_figure(df, x_col: str, y_col: str, title: str):
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return None

    fig = px.line(df, x=x_col, y=y_col, title=title)
    fig.update_layout(xaxis_title=x_col, yaxis_title=y_col)
    return fig
