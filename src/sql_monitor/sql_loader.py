from pathlib import Path


SQL_DIR = Path(__file__).resolve().parent / "sql"


def load_sql(*parts: str) -> str:
    return SQL_DIR.joinpath(*parts).read_text(encoding="utf-8").strip()


def load_sql_statements(*parts: str) -> list[str]:
    text = load_sql(*parts)
    return [statement.strip() for statement in text.split(";") if statement.strip()]
