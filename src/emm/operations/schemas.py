from sqlalchemy import exists, select, text

from src.emm.engine.data import DDLTableContext
from src.emm.engine.parser import (
    extract_create_statement,
    parse_create_statement,
    read_ddl_for_project,
)
from src.emm.models.database_base import context_session
from src.emm.models.schema import Schema
from src.emm.operations.constants import PG_STAT_STATEMENTS


def load_schemas() -> list[Schema]:
    with context_session() as session:
        stmt = select(Schema)
        return session.scalars(stmt)


def initialize_schema(project_name: str):
    """
    This is called after validation, no need to double check that the file exists
    """
    ddl = read_ddl_for_project(project_name=project_name)

    # Parse the DDL into a statement object
    create_statement = extract_create_statement(ddl)

    # Get the context, so that we can easily create permutations
    context: DDLTableContext = parse_create_statement(
        project_name=project_name, statement=create_statement
    )

    with context_session() as session:
        # Raise an error if such schema is already present
        if session.execute(
            select(Schema).where(exists().where(Schema.name == project_name))
        ).scalar():
            raise Exception(
                f"Schema {project_name} already present. "
                "Please use a different name or clean "
                f"{project_name} first before initializing it again"
            )

        # Create the schema
        session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {project_name}"))

        # Set the search path to the new schema
        session.execute(text(f"SET search_path TO {project_name}"))

        # Execute the DDL
        session.execute(text(ddl))

        session.execute(text("SET search_path TO public"))

        # Create the object
        session.add(
            Schema(
                name=project_name,
                original_table_name=context.table_name,
            )
        )


def delete_schema(schema: Schema):
    """
    Drop the schema and delete the associated entry from the schema table.
    """
    with context_session() as session:
        session.delete(schema)
        session.execute(text(f"DROP SCHEMA IF EXISTS {schema.name} CASCADE"))


def find_schema_by_name(schema_name: str) -> Schema | None:
    """
    Loads a Schema based on the name. If not found, returns None.
    """
    with context_session() as session:
        return session.scalars(
            select(Schema).where(Schema.name == schema_name)
        ).one_or_none()
