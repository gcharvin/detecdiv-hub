from pathlib import Path
import sys

from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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
    schema_path = REPO_ROOT / "db" / "schema.sql"
    schema_text = schema_path.read_text(encoding="utf-8")
    statements = iter_sql_statements(schema_text)

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    print(f"Applied schema from {schema_path}")


if __name__ == "__main__":
    main()
