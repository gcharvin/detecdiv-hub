from pathlib import Path

from sqlalchemy import text

from api.db import engine


def iter_sql_statements(schema_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in schema_text.splitlines():
        current.append(line)
        if line.rstrip().endswith(";"):
            statement = "\n".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
    trailing = "\n".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "db" / "schema.sql"
    schema_text = schema_path.read_text(encoding="utf-8")
    statements = iter_sql_statements(schema_text)

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    print(f"Applied schema from {schema_path}")


if __name__ == "__main__":
    main()

