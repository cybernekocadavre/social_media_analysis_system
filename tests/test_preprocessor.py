from src.text_preprocessing import clean_text, normalize_text


def test_clean_text_removes_links():
    text = "Check this http://example.com now!"
    result = clean_text(text)
    assert "http" not in result


def test_normalize_text_keeps_meaningful_words():
    text = "This is a simple market analysis text"
    result = normalize_text(text)
    assert "analysis" in result
    assert "text" in result