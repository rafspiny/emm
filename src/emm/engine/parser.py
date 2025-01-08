import logging
import os
import re

import sqlparse
from sqlparse.sql import Identifier, Parenthesis, Statement
from sqlparse.tokens import DDL, Keyword, Name, Punctuation

from src.emm.engine.data import DDLTableColumn, DDLTableContext, ParsingContext

log = logging.getLogger(__name__)


def read_ddl_for_project(project_name: str) -> str:
    """
    Returns the DDL associated with the main table of the specified project
    """
    # FIXME Instead of just reading the file, get the DDL from the table from the DB
    return read_sql_file(project_name, "schema")


def read_data_for_project(project_name: str) -> str:
    """
    Returns the content of the data to insert into the project tables
    """
    return read_sql_file(project_name, "data")


def read_sql_file(project_name: str, filename_to_load: str) -> str:
    """
    Access the specified file in the project folder and return it as a string
    """
    schema_file_path = os.path.join(
        "sql", "projects", project_name, f"{filename_to_load}.sql"
    )
    content: str

    log.debug(f"Loading DDL for project {project_name} at location {schema_file_path}")

    # Read the DDL from schema.sql
    with open(schema_file_path, "r") as file:
        content = file.read()
        return content


def parse_all_statements(ddl: str) -> list[Statement]:
    """
    Parse all statements present int he file.
    """
    parsed_statements: list[Statement] = sqlparse.parse(ddl)

    log.debug(f"Found {len(parsed_statements)} statements")

    return parsed_statements


def is_statement_create_table(statement: Statement) -> bool:
    """
    Check if the statement under evaluation is a create table one. It relies on re.
    """
    create_table_re = re.compile(r".*CREATE\s+TABLE", re.I | re.M | re.S)
    return create_table_re.match(statement.value) is not None


def extract_create_statement(ddl: str) -> Statement:
    """
    Parse the str representing the DDL and returns the first create table statement
    """
    parsed_statements = parse_all_statements(ddl)
    create_table_statements: list[Statement] = [
        statement
        for statement in parsed_statements
        if is_statement_create_table(statement)
    ]

    if len(create_table_statements) == 0:
        raise ValueError("No `CREATE TABLE` statement found.")
    elif len(create_table_statements) > 1:
        log.info(
            "Found more CREATE STATEMENT in the file, considering the first and discarding the others."
        )

    return create_table_statements[0]


def parse_create_statement(project_name: str, statement: Statement) -> DDLTableContext:
    """
    Builds a DDLTableContext object describing the table being created in the statement passed as argument.
    The context has all the info necessary to compute the permutations.
    """
    parsing_context: ParsingContext = ParsingContext(statement.tokens)
    context: DDLTableContext = DDLTableContext(project_name=project_name)

    _parse_create(parsing_context, context)

    return context


def _parse_create(parsing_context: ParsingContext, context: DDLTableContext) -> None:
    """
    A tiny and simple parser that keep the state of the parsing and fill the context with the columns found.
    """
    has_found_identifier: bool = False
    current_identifier: str | None = None
    current_identifier_type: str | None = None
    for token in parsing_context.tokens:
        if parsing_context.in_columns:
            if not has_found_identifier and token.is_whitespace:
                # Skip spaces before the identifiers
                continue

            if token.ttype is Punctuation and token.normalized == "(":
                # Skip the beginning of the columns section
                continue

            if isinstance(token, sqlparse.sql.Comment):
                # Skip comments
                continue

            if (
                has_found_identifier
                and token.ttype is Punctuation
                and token.normalized in [",", ")"]
                and current_identifier
                and current_identifier_type
            ):
                # Once we hit the comma, save the information accumulated so far on the column and reset
                # the information we have gathered so far, as we will find another column.
                context.add_column(
                    DDLTableColumn(
                        name=current_identifier,
                        column_type=current_identifier_type,
                        original_definition="".join(
                            parsing_context.current_column_definition
                        ),
                    )
                )
                has_found_identifier = False
                current_identifier = None
                current_identifier_type = None
                parsing_context.current_column_definition = []

            if isinstance(token, Identifier):
                # Save the name of the column
                log.info(f"Found column {token.normalized}")
                has_found_identifier = True
                current_identifier = token.normalized

            if has_found_identifier and token.ttype is Name.Builtin:
                current_identifier_type = token.normalized

            # Add the token to the information of the column
            if has_found_identifier:
                parsing_context.current_column_definition.append(token.normalized)

        if token.ttype is DDL and token.normalized == "CREATE":
            parsing_context.in_columns = False
            parsing_context.in_create = True
            parsing_context.in_create_table = False
            log.debug("Found a CREATE statement")
        if (
            parsing_context.in_create
            and token.ttype is Keyword
            and token.normalized == "TABLE"
        ):
            parsing_context.in_create_table = True
            log.debug("Identified a CREATE TABLE statement")
        if parsing_context.in_create_table and isinstance(token, Identifier):
            context.table_name = token.value
            parsing_context.in_create = False
            log.info(f"Found table name {token.value}")
        if token.is_group and isinstance(token, Parenthesis):
            column_parsing_context: ParsingContext = ParsingContext(token.tokens)
            column_parsing_context.in_columns = True
            _parse_create(parsing_context=column_parsing_context, context=context)
            column_parsing_context.in_columns = False
            # I could have taken the identifiers just with [token for token in parsing_context.tokens
            # if isinstance(token, Identifier)]
