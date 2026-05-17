import os
from pathlib import Path
from typing import Set

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = BASE_DIR / "data" / "raw" / "pikabu_for_vkr.csv"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_CHARTS_DIR = BASE_DIR / "output" / "charts"
OUTPUT_TABLES_DIR = BASE_DIR / "output" / "tables"
OUTPUT_REPORTS_DIR = BASE_DIR / "output" / "reports"

# По умолчанию читаем ограниченную выборку, чтобы прототип быстро запускался.
# Для полного анализа можно передать nrows=None в pipeline/Streamlit.
TEST_ROWS = int(os.getenv("TEST_ROWS", "5000"))

# Настройки чтения файлов
AUTO_DETECT_CSV_SEPARATOR = True
CSV_SEPARATOR = ","
CSV_ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "cp1251", "latin-1"]
CSV_DECIMAL = "."
CSV_QUOTECHAR = '"'
CSV_LOW_MEMORY = False
EXCEL_SHEET_NAME = 0
JSON_ORIENT = None

SAVE_INTERMEDIATE_FILES = True

# PostgreSQL
USE_POSTGRES = os.getenv("USE_POSTGRES", "true").strip().lower() in {"1", "true", "yes", "y"}
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "social_media_analysis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "social_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "social_pass_2026")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

# Plotly
USE_PLOTLY = True
SAVE_PLOTLY_HTML = True
SAVE_PLOTLY_JSON = True
AUTO_DETECT_COLUMNS = True

# Канонические названия полей
# post_id, user_id, datetime, title, text, likes_count, comments_count,
# reposts_count, source, company, url, category

COLUMN_MAPPING = {
    "id": "post_id",
    "author": "user_id",
    "created_datetime": "datetime",
    "created_utc": "datetime",
    "title": "title",
    "text": "text",
    "score": "likes_count",
    "num_comments": "comments_count",
    "subreddit": "source",
    "company": "company",
    "url": "url",
    "permalink": "url",
    "flair": "category",
}

COLUMN_ALIASES = {
    "post_id": [
        "post_id", "postid", "id", "record_id", "row_id", "message_id"
    ],
    "user_id": [
        "user_id", "userid", "user", "author", "author_id", "username",
        "screen_name", "creator_id", "user_posted"
    ],
    "datetime": [
        "datetime", "created_datetime", "created_utc", "created_at",
        "timestamp", "date_time", "published_at", "publication_time",
        "time", "date_posted", "date", "created", "posted_at"
    ],
    "title": [
        "title", "headline", "subject", "post_title", "name"
    ],
    "text": [
        "text", "body", "content", "message", "post", "comment",
        "description", "selftext", "caption", "full_text"
    ],
    "likes_count": [
        "likes_count", "likes", "score", "upvotes", "favorites",
        "favorite_count", "favourites", "upvote_count", "num_upvotes",
        "reactions", "reaction_count"
    ],
    "comments_count": [
        "comments_count", "comment_count", "comments", "num_comments",
        "replies_count", "reply_count"
    ],
    "reposts_count": [
        "reposts_count", "reposts", "shares", "share_count", "retweets",
        "retweet_count", "reshares"
    ],
    "source": [
        "source", "subreddit", "platform", "channel", "community",
        "forum", "community_name", "group", "page"
    ],
    "company": [
        "company", "ticker", "symbol", "brand", "organization", "organisation"
    ],
    "url": [
        "url", "link", "permalink", "post_url"
    ],
    "category": [
        "category", "flair", "tag", "topic", "label", "type"
    ],
}

REQUIRED_CANONICAL_COLUMNS = [
    "post_id",
    "user_id",
    "datetime",
]

TEXT_SOURCE_COLUMNS = ["title", "text"]

OPTIONAL_COLUMNS_DEFAULTS = {
    "title": "",
    "text": "",
    "likes_count": 0,
    "comments_count": 0,
    "reposts_count": 0,
    "source": "unknown",
    "company": "unknown",
    "url": "",
    "category": "unknown",
}

NUMERIC_COLUMNS = [
    "likes_count",
    "comments_count",
    "reposts_count",
]

# Настройки времени
DATETIME_UNIT = None
DATETIME_FORMAT = None
DATETIME_DAYFIRST = False
DATETIME_UTC = False

# Предобработка текста
DROP_ROWS_WITH_EMPTY_TEXT = True
MIN_TOKEN_LENGTH = 3

CUSTOM_STOPWORDS: Set[str] = {
    # Русские служебные слова
    "а", "без", "более", "больше", "будет", "будто", "бы", "был", "была", "были",
    "было", "быть", "вам", "вас", "вдруг", "ведь", "весь", "вместе", "вместо",
    "вне", "во", "вот", "впрочем", "все", "всего", "всем", "всеми", "всему",
    "всех", "всею", "всю", "вся", "всё", "где", "да", "даже", "для", "до",
    "его", "ее", "её", "если", "есть", "еще", "ещё", "же", "за", "зачем",
    "здесь", "значит", "из", "или", "им", "иногда", "их", "как", "какая",
    "какие", "какой", "когда", "конечно", "кто", "куда", "ли", "либо", "лучше",
    "между", "меня", "мне", "много", "может", "можно", "мой", "моя", "мы",
    "на", "над", "надо", "нас", "наш", "не", "него", "нее", "неё", "ней",
    "нельзя", "нет", "ни", "нибудь", "никогда", "ним", "них", "но", "ну",
    "об", "один", "одна", "одни", "одно", "он", "она", "они", "оно", "от",
    "очень", "по", "под", "после", "потом", "потому", "почему", "при", "про",
    "раз", "разве", "себе", "себя", "сегодня", "сейчас", "со", "среди", "так",
    "такая", "такие", "такой", "там", "тебя", "тем", "теперь", "то", "тогда",
    "того", "тоже", "только", "том", "тот", "тут", "ты", "уже", "хотя", "чего",
    "чем", "через", "что", "чтоб", "чтобы", "чуть", "эта", "эти", "этим",
    "этих", "это", "этого", "этой", "этом", "этот", "эту", "я",

    # Частые разговорные/технические слова без аналитической ценности
    "quot", "amp", "nbsp", "lt", "gt", "https", "http", "www", "com", "ru",
    "ещe", "хоть", "вообще", "просто", "типа", "вроде", "например", "короче",
    "ладно", "ага", "ах", "ох", "эх", "ха", "ахах", "ахаха", "лол",

    # Английские частые слова
    "the", "and", "or", "but", "if", "then", "else", "when", "where", "what",
    "who", "why", "how", "this", "that", "these", "those", "there", "here",
    "with", "without", "from", "into", "onto", "for", "you", "your", "yours",
    "are", "was", "were", "been", "being", "have", "has", "had", "not", "yes",
    "no", "just", "very", "really", "also", "only", "can", "could", "would",
    "should", "will", "shall", "may", "might", "must"
}

# Отчеты по качеству предобработки
PREPROCESSING_QUALITY_TXT_FILENAME = "preprocessing_quality_report.txt"
PREPROCESSING_QUALITY_CSV_FILENAME = "preprocessing_quality_report.csv"

# Параметры аналитики
TOP_N_WORDS = 20
TOP_N_BIGRAMS = 20
TOP_N_TRIGRAMS = 20
TOP_N_USERS = 20
TOP_N_FIELDS = 15

# Тренды по времени
TOP_N_TREND_TERMS = 30
TOP_N_GROWING_TERMS = 20

# Тематическое моделирование
N_TOPICS = 5
TOP_N_TOPIC_TERMS = 8
TOPIC_MODEL_MAX_FEATURES = 1500

# Универсальная тональность
SENTIMENT_POSITIVE_WORDS = {
    "good", "great", "excellent", "positive", "success", "successful",
    "improve", "improved", "improving", "best", "better", "strong",
    "happy", "love", "liked", "like", "benefit", "beneficial", "win",
    "winner", "support", "safe", "stable", "clear", "useful", "interesting",
    "важно", "хорошо", "хороший", "хорошая", "лучше", "лучший", "успех",
    "успешно", "нравится", "полезно", "поддержка", "сильный", "стабильно",
}
SENTIMENT_NEGATIVE_WORDS = {
    "bad", "terrible", "awful", "negative", "fail", "failed", "failure",
    "problem", "problems", "issue", "issues", "worse", "worst", "weak",
    "angry", "hate", "hated", "risk", "risky", "danger", "dangerous",
    "bug", "broken", "unclear", "useless", "loss", "drop", "decline",
    "плохо", "плохой", "плохая", "хуже", "худший", "ошибка", "ошибки",
    "проблема", "проблемы", "риск", "опасно", "ненавижу", "сломано", "провал",
}
SENTIMENT_POSITIVE_THRESHOLD = 0.05
SENTIMENT_NEGATIVE_THRESHOLD = -0.05

# Сегментация пользователей
USER_SEGMENT_CLUSTERS = 3
RANDOM_STATE = 42

ENGAGEMENT_BINS = [-1, 0, 5, 20, 100, 1000000]
ENGAGEMENT_LABELS = ["0", "1-5", "6-20", "21-100", "100+"]

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_TABLES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
