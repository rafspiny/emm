import pytest

from src.emm.engine.parser import extract_create_statement


@pytest.mark.parametrize(
    "sql_str,raises_exception",
    [
        (
            "create table if not exists table_name ( id SERIAL PRIMARY KEY, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, name TEXT NOT NULL, original_table_name TEXT NOT NULL;",
            True,
        ),
        (
            "create table if not exists table_name ( id SERIAL PRIMARY KEY, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, name TEXT NOT NULL, original_table_name TEXT NOT NULL",
            True,
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS original_table (
                id SERIAL PRIMARY KEY,                       -- Unique identifier for each row, automatically incremented
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp column for creation time, defaults to current time
                name TEXT NOT NULL,                          -- Name of the project
                original_table_name TEXT NOT NULL            -- The original table name for the project
            );
            """,
            True,
        ),
    ],
)
def test_extract_create_statement(sql_str: str, raises_exception: bool):
    extract_create_statement(sql_str)


def test_extract_create_statement_fails_no_create_statement():
    with pytest.raises(ValueError):
        extract_create_statement("select * from table_name;")
