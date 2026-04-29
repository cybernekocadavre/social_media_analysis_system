import re
from typing import List

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from src.config import CUSTOM_STOPWORDS, MIN_TOKEN_LENGTH

STOPWORDS = set(ENGLISH_STOP_WORDS) | {word.lower() for word in CUSTOM_STOPWORDS}


def clean_text(text: str) -> str:
    text = str(text).lower()

    text = re.sub(r"http\S+|www\S+", " ", text)
    text = re.sub(r"&amp;", " ", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"[_/\\|+-]+", " ", text)
    text = re.sub(r"[^0-9a-zA-Zа-яА-ЯёЁ\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def tokenize_text(text: str) -> List[str]:
    tokens = text.split()
    tokens = [
        token for token in tokens
        if token not in STOPWORDS
        and len(token) >= MIN_TOKEN_LENGTH
        and not token.isdigit()
    ]
    return tokens


def normalize_text(text: str) -> str:
    cleaned = clean_text(text)
    tokens = tokenize_text(cleaned)
    return " ".join(tokens)
