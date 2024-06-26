import os

from sqlalchemy import exists, select, text

from src.emm.models.database_base import context_session
from src.emm.models.schema import Schema


def load_schemas() -> list[Schema]:
    with context_session() as session:
        stmt = select(Schema)
        return session.scalars(stmt)


def initialize_schema(project_name: str):
    """
    This is called after validation, no need to double check that the file exists
    """
    # FIXME Instead of just executing the sql file, parse it and create the entity

    schema_file_path = os.path.join("sql", "projects", project_name, "schema.sql")
    ddl: str

    # Read the DDL from schema.sql
    with open(schema_file_path, "r") as file:
        ddl = file.read()

    with context_session() as session:
        # Raise an error if such schema is already present
        if session.execute(
            select(Schema).where(exists().where(Schema.name == "emm_schema"))
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

        # Create the object
        session.add(
            Schema(
                name=project_name,
                is_populated=False,
                is_permutation=False,
                permutation_id=None,
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
        return session.scalars(select(Schema).where(Schema.name == schema_name))


def populate_schema(schema: Schema, only_original: bool) -> None:
    """
    TODO Load the sql file with data and import it in the original table.
    If only_original is False, populate the permutations too.
    The first operation done is t truncate all the table. All the table,
    according to only_original.

    # FIXME We should parse the insert statements and change the order
    before executing it.
    # FIXME We should not need a file. We could generate the data on the fly.
    """
    pass
