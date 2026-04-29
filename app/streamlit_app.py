from pathlib import Path
import sys
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import streamlit as st

from src.analysis_service import run_full_analysis
from src.config import COLUMN_ALIASES, COLUMN_MAPPING, RAW_DATA_PATH, TEST_ROWS, USE_POSTGRES
from src.data_loader import read_raw_dataset
from src.db import check_connection
from src.db_init import initialize_postgres
from src.db_reader import (
    read_latest_run_id,
    read_recent_runs,
    read_table_count,
    read_table_names,
    read_table_preview,
    table_has_run_id,
)
from src.db_writer import save_analysis_result_to_postgres
from src.plotly_visualizer import make_bar_figure, make_line_figure
from src.schema import normalize_column_name


def get_default_raw_column(canonical_field: str, raw_columns: list[str]) -> str:
    preferred_candidates = []

    for raw_name, canonical_name in COLUMN_MAPPING.items():
        if canonical_name == canonical_field:
            preferred_candidates.append(raw_name)

    preferred_candidates.extend(COLUMN_ALIASES.get(canonical_field, []))
    preferred_candidates.append(canonical_field)

    normalized_raw_columns = {normalize_column_name(col): col for col in raw_columns}

    for candidate in preferred_candidates:
        normalized_candidate = normalize_column_name(candidate)
        if normalized_candidate in normalized_raw_columns:
            return normalized_raw_columns[normalized_candidate]

    return "<не использовать>"


def get_source_name(source: Any) -> str:
    if isinstance(source, (str, Path)):
        return Path(source).name
    if hasattr(source, "name"):
        return str(source.name)
    return "uploaded_file"


def resolve_nrows(option: str) -> Optional[int]:
    if option == "Все строки":
        return None
    return int(option.replace(" ", ""))


def dataframe_to_csv_bytes(df) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def show_table(title: str, df, file_name: str) -> None:
    st.subheader(title)
    if df is None or df.empty:
        st.info("Нет данных для отображения.")
        return

    st.dataframe(df, width="stretch")
    st.download_button(
        label=f"Скачать CSV: {title}",
        data=dataframe_to_csv_bytes(df),
        file_name=file_name,
        mime="text/csv",
        key=f"download_{file_name}",
    )


def run_analysis(source, custom_mapping, nrows_limit, save_to_postgres_enabled):
    analysis_result = run_full_analysis(
        source=source,
        nrows=nrows_limit,
        custom_mapping=custom_mapping,
    )

    run_id = None
    postgres_warning = None
    if USE_POSTGRES and save_to_postgres_enabled:
        try:
            initialize_postgres()
            run_id = save_analysis_result_to_postgres(
                analysis_result,
                source_name=get_source_name(source),
                nrows_limit=nrows_limit,
            )
        except Exception as error:
            postgres_warning = str(error)

    st.session_state["analysis_ready"] = True
    st.session_state["analysis_result"] = analysis_result
    st.session_state["run_id"] = run_id
    st.session_state["postgres_warning"] = postgres_warning


st.set_page_config(page_title="Система анализа данных социальных сетей", layout="wide")
st.title("Система анализа данных социальных сетей")
st.caption("Прототип для загрузки CSV/XLSX/JSON, предобработки, анализа трендов, тем, тональности и поведения пользователей")

if "analysis_ready" not in st.session_state:
    st.session_state["analysis_ready"] = False

st.sidebar.header("Источник данных")

source_mode = st.sidebar.radio(
    "Выбор источника",
    ["Использовать файл из config.py", "Загрузить свой файл"],
)

uploaded_file = None
source = RAW_DATA_PATH

if source_mode == "Загрузить свой файл":
    uploaded_file = st.sidebar.file_uploader(
        "Загрузите CSV/XLSX/JSON файл",
        type=["csv", "xlsx", "xls", "json"],
    )

    if uploaded_file is not None:
        source = uploaded_file
    else:
        st.warning("Файл не загружен. Пока используется путь из config.py.")

nrows_option = st.sidebar.selectbox(
    "Размер выборки для анализа",
    options=[str(TEST_ROWS), "20 000", "50 000", "Все строки"],
    index=0,
)
nrows_limit = resolve_nrows(nrows_option)

save_to_postgres_enabled = st.sidebar.checkbox(
    "Сохранять результат запуска в PostgreSQL",
    value=USE_POSTGRES,
)

manual_mapping_enabled = st.sidebar.checkbox(
    "Настроить сопоставление колонок вручную",
    value=False,
)

custom_mapping = None

if manual_mapping_enabled:
    try:
        raw_df = read_raw_dataset(source=source, nrows=100)
        raw_columns = list(raw_df.columns)

        st.sidebar.markdown("### Ручное сопоставление колонок")
        st.sidebar.caption("Можно ничего не менять — тогда останется автоопределение и config.py.")

        selectable_options = ["<не использовать>"] + raw_columns
        canonical_fields = [
            "post_id",
            "user_id",
            "datetime",
            "title",
            "text",
            "likes_count",
            "comments_count",
            "reposts_count",
            "source",
            "company",
            "url",
            "category",
        ]

        custom_mapping = {}

        for canonical_field in canonical_fields:
            default_value = get_default_raw_column(canonical_field, raw_columns)
            default_index = selectable_options.index(default_value) if default_value in selectable_options else 0

            selected_raw = st.sidebar.selectbox(
                f"{canonical_field}",
                options=selectable_options,
                index=default_index,
                key=f"mapping_{canonical_field}",
            )

            if selected_raw != "<не использовать>":
                custom_mapping[selected_raw] = canonical_field

    except Exception as error:
        st.sidebar.error(f"Не удалось прочитать файл для настройки колонок: {error}")
        custom_mapping = None

if USE_POSTGRES:
    if st.sidebar.button("Проверить подключение к PostgreSQL"):
        try:
            check_connection()
            st.sidebar.success("Подключение к PostgreSQL успешно")
        except Exception as error:
            st.sidebar.error(f"Ошибка подключения: {error}")

if st.button("Запустить анализ"):
    try:
        run_analysis(
            source=source,
            custom_mapping=custom_mapping,
            nrows_limit=nrows_limit,
            save_to_postgres_enabled=save_to_postgres_enabled,
        )
        st.success("Анализ успешно выполнен")
        if st.session_state.get("run_id") is not None:
            st.info(f"Результаты сохранены в PostgreSQL, run_id={st.session_state['run_id']}")
        if st.session_state.get("postgres_warning"):
            st.warning(
                "Анализ выполнен, но сохранить результаты в PostgreSQL не удалось: "
                f"{st.session_state['postgres_warning']}"
            )
    except Exception as error:
        st.session_state["analysis_ready"] = False
        st.error(f"Ошибка при выполнении анализа: {error}")

analysis_result = st.session_state.get("analysis_result")
tables = analysis_result.get("tables", {}) if analysis_result else {}
summary = analysis_result.get("summary", {}) if analysis_result else {}

tab1, tab2, tab3, tab4 = st.tabs(["Сводка и таблицы", "Тренды и темы", "Plotly", "PostgreSQL"])

with tab1:
    if not st.session_state["analysis_ready"]:
        st.info("Сначала нажмите кнопку «Запустить анализ».")
    else:
        st.subheader("Краткая сводка")
        st.json(summary)

        show_table("Первые строки обработанного набора", analysis_result["processed_df"].head(50), "processed_preview.csv")

        st.subheader("Служебная информация о сопоставлении колонок")
        st.json(analysis_result.get("matched_columns", {}))

        show_table("Журнал качества предобработки", analysis_result["quality_report"], "quality_report.csv")

        col1, col2 = st.columns(2)
        with col1:
            show_table("Частотные слова", tables.get("top_words"), "top_words.csv")
            show_table("Биграммы", tables.get("top_bigrams"), "top_bigrams.csv")
            show_table("Триграммы", tables.get("top_trigrams"), "top_trigrams.csv")
            show_table("TF-IDF термины", tables.get("tfidf_terms"), "tfidf_terms.csv")
        with col2:
            show_table("Топ источников", tables.get("top_sources"), "top_sources.csv")
            show_table("Топ компаний / объектов", tables.get("top_companies"), "top_companies.csv")
            show_table("Топ категорий", tables.get("top_categories"), "top_categories.csv")

        show_table("Активные пользователи", tables.get("top_users"), "top_users.csv")
        show_table("Пользователи по вовлечённости", tables.get("top_users_engagement"), "top_users_engagement.csv")
        show_table("Распределение активности пользователей", tables.get("user_distribution"), "user_distribution.csv")
        show_table("Активность пользователей по месяцам", tables.get("user_activity_by_month"), "user_activity_by_month.csv")
        show_table("Распределение вовлечённости", tables.get("engagement_distribution"), "engagement_distribution.csv")
        show_table("Сегменты пользователей", tables.get("user_segments"), "user_segments.csv")
        show_table("Распределение сегментов пользователей", tables.get("user_segment_distribution"), "user_segment_distribution.csv")

with tab2:
    if not st.session_state["analysis_ready"]:
        st.info("Сначала нажмите кнопку «Запустить анализ».")
    else:
        st.markdown("### Тренды во времени")
        show_table("Публикации по датам", tables.get("posts_by_date"), "posts_by_date.csv")
        show_table("Публикации по месяцам", tables.get("posts_by_month"), "posts_by_month.csv")
        show_table("Термины по месяцам", tables.get("terms_by_month"), "terms_by_month.csv")
        show_table("Растущие термины", tables.get("growing_terms"), "growing_terms.csv")

        st.markdown("### Тематическое моделирование")
        show_table("Слова тем", tables.get("topic_terms"), "topic_terms.csv")
        show_table("Темы постов", tables.get("post_topics"), "post_topics.csv")
        show_table("Сводка по темам", tables.get("topic_summary"), "topic_summary.csv")
        show_table("Темы по месяцам", tables.get("topics_by_month"), "topics_by_month.csv")

        st.markdown("### Универсальная тональность")
        st.caption(
            "Тональность считается по общему словарю позитивных и негативных слов, "
            "без привязки к финансовой предметной области."
        )
        show_table("Распределение тональности", tables.get("sentiment_distribution"), "sentiment_distribution.csv")
        show_table("Тональность по месяцам", tables.get("sentiment_by_month"), "sentiment_by_month.csv")
        show_table("Тональность по источникам", tables.get("sentiment_by_source"), "sentiment_by_source.csv")

with tab3:
    if not st.session_state["analysis_ready"]:
        st.info("Сначала нажмите кнопку «Запустить анализ».")
    else:
        figure_specs = [
            (make_bar_figure(tables.get("top_words"), "word", "count", "Частотные слова"), "Частотные слова"),
            (make_line_figure(tables.get("posts_by_date"), "date", "post_count", "Публикации по датам"), "Публикации по датам"),
            (make_line_figure(tables.get("posts_by_month"), "month", "post_count", "Публикации по месяцам"), "Публикации по месяцам"),
            (make_bar_figure(tables.get("growing_terms"), "term", "absolute_growth", "Растущие термины"), "Растущие термины"),
            (make_bar_figure(tables.get("topic_summary"), "topic_label", "posts_count", "Сводка по темам"), "Сводка по темам"),
            (make_bar_figure(tables.get("sentiment_distribution"), "sentiment_label", "posts_count", "Распределение тональности"), "Распределение тональности"),
            (make_bar_figure(tables.get("top_sources"), "source", "count", "Топ источников"), "Топ источников"),
            (make_bar_figure(tables.get("top_categories"), "category", "count", "Топ категорий"), "Топ категорий"),
            (make_bar_figure(tables.get("top_users"), "user_id", "posts_count", "Активные пользователи"), "Активные пользователи"),
            (make_bar_figure(tables.get("user_distribution"), "activity_group", "users_count", "Распределение активности"), "Распределение активности"),
            (make_bar_figure(tables.get("engagement_distribution"), "engagement_group", "posts_count", "Распределение вовлечённости"), "Распределение вовлечённости"),
            (make_bar_figure(tables.get("user_segment_distribution"), "segment_label", "users_count", "Сегменты пользователей"), "Сегменты пользователей"),
            (make_line_figure(tables.get("user_activity_by_month"), "month", "total_posts", "Активность пользователей по месяцам"), "Активность пользователей по месяцам"),
        ]

        for fig, title in figure_specs:
            if fig is not None:
                st.plotly_chart(fig, width="stretch")

with tab4:
    if not USE_POSTGRES:
        st.info("Поддержка PostgreSQL отключена в config.py")
    else:
        try:
            st.subheader("Последние запуски анализа")
            recent_runs = read_recent_runs(limit=10)
            st.dataframe(recent_runs, width="stretch")

            latest_run_id = read_latest_run_id()
            if latest_run_id is not None:
                st.caption(f"Последний run_id: {latest_run_id}")

            st.subheader("Таблицы в PostgreSQL")
            table_names_df = read_table_names()

            if table_names_df.empty:
                st.info("В схеме PostgreSQL пока нет таблиц.")
            else:
                table_names = table_names_df["table_name"].tolist()

                selected_table = st.selectbox(
                    "Выберите таблицу для просмотра",
                    options=table_names,
                    index=0,
                    key="postgres_table_select",
                )

                latest_only = st.checkbox(
                    "Показывать только строки последнего запуска",
                    value=True,
                    key="postgres_latest_only",
                )

                selected_run_id = latest_run_id if latest_only else None
                if selected_run_id is not None and not table_has_run_id(selected_table):
                    selected_run_id = None

                row_count = read_table_count(selected_table, run_id=selected_run_id)
                st.write(f"Количество строк в таблице **{selected_table}**: {row_count}")

                if row_count == 0:
                    st.info("Выбранная таблица пуста.")
                else:
                    max_preview = min(row_count, 100)
                    default_preview = min(20, max_preview)

                    if max_preview <= 1:
                        preview_limit = 1
                        st.caption("В таблице доступна только 1 строка для предпросмотра.")
                    else:
                        preview_limit = st.slider(
                            "Количество строк для предпросмотра",
                            min_value=1,
                            max_value=max_preview,
                            value=default_preview,
                            step=1 if max_preview < 10 else 5,
                            key="postgres_preview_limit",
                        )

                    preview_df = read_table_preview(
                        table_name=selected_table,
                        limit=preview_limit,
                        run_id=selected_run_id,
                    )

                    st.subheader(f"Предпросмотр таблицы: {selected_table}")
                    st.dataframe(preview_df, width="stretch")

        except Exception as error:
            st.warning(f"Не удалось прочитать данные из PostgreSQL: {error}")
