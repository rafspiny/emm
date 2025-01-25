import re

from sqlalchemy import text

from src.emm.engine.parser import read_data_for_project
from src.emm.models.database_base import context_session
from src.emm.models.schema import Permutation, Schema
from src.emm.operations.permutations import load_permutations


def populate_table_with_data(
    schema: Schema, permutation: Permutation, insert_data: str
):
    """
    Execute the insert on the table specified in permutation.
    Replace all instances of `INSERT INTO xxx` into `INSERT INTO schema.permutation.name`.
    Then execute the sql str.
    """
    # Replace the table name
    insert_data = re.sub(
        r"INSERT INTO [a-zA-Z0-9_]+", f"INSERT INTO {permutation.name}", insert_data
    )

    # Execute the insert
    with context_session() as session:
        # Set the search path to the new schema
        session.execute(text(f"SET search_path TO {schema.name}"))

        # Execute the DDL
        session.execute(text(insert_data))

        session.execute(text("SET search_path TO public"))

        # Lastly, save the fact that the permutation ot populated.
        permutation.is_populated = True
        session.add(permutation)
        session.commit()


def populate_permutation(permutation_key: str, schema: Schema) -> None:
    # Get the permutation
    with context_session() as session:
        permutation = (
            session.query(Permutation)
            .filter(
                Permutation.schema_id == schema.id,
                Permutation.permutation_code == permutation_key,
            )
            .one()
        )

    # Get the DDL
    with context_session() as session:
        ddl = session.execute(
            text(
                "SELECT ddl FROM emm.permutation WHERE schema_id = :schema_id "
                "AND permutation_id = :permutation_id"  # nosec
            ),
            {"schema_id": schema.id, "permutation_id": permutation_key},
        ).scalar()

    # Execute the DDL
    with context_session() as session:
        session.execute(text(ddl))

    # Populate the permutation
    with context_session() as session:
        session.execute(
            text("SELECT emm.populate_permutation(:schema_id, :permutation_id)"),
            {"schema_id": schema.id, "permutation_id": permutation_key},
        )

    # Update the permutation
    with context_session() as session:
        permutation.is_populated = True
        session.add(permutation)


def save_permutation(
    permutation_key: str, permutation_ddl: str, schema: Schema
) -> None:
    # Write create statement
    with context_session() as session:
        # Execute the DDL
        session.execute(text(permutation_ddl))

        # Create the object
        session.add(
            Permutation(
                is_permutation=True,
                is_populated=False,
                schema_id=schema.id,
                schema=schema,
                permutation_id=permutation_key,
                name=permutation_key,
            )
        )


def populate_schema(schema: Schema, only_original: bool) -> None:
    """
    Load the sql file with data and import it in the original table.
    If only_original is False, populate the permutations too.
    The first operation done is t truncate all the table. All the table,
    according to only_original.

    # FIXME We should parse the insert statements and change the order
    before executing it.
    # TODO We should not need a file. We could generate the data on the fly.
    """
    insert_data: str = read_data_for_project(schema.name)
    permutations = load_permutations(schema, only_original)
    for permutation in permutations:
        populate_table_with_data(schema, permutation, insert_data)
