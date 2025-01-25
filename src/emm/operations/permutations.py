import itertools
from collections import defaultdict
from typing import Iterable

from sqlalchemy import text

from src.emm.engine.data import (
    DDLTableColumn,
    DDLTableContext,
    PermutationRequest,
    PermutationSettings,
)
from src.emm.engine.parser import (
    extract_create_statement,
    parse_create_statement,
    read_ddl_for_project,
)
from src.emm.models.database_base import context_session
from src.emm.models.schema import Permutation, Schema
from src.emm.operations.schemas import find_schema_by_name

"""
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣤⠶⠶⠾⠿⠛⠛⠛⠛⠓⠒⠲⠶⠤⣤⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⡴⠟⠋⣁⣤⣴⣶⣶⡶⠶⠖⠒⠂⠀⠀⠀⠀⠀⠀⠈⠉⠛⠷⣦⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠾⠋⣠⣴⢟⣿⣿⡿⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠲⣤⡙⠻⣶⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠃⣠⣾⢟⣴⣿⡿⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣷⡌⠻⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⡾⠁⣰⣿⣿⣾⣿⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⡀⠘⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⣾⠁⢀⣿⣿⣿⣿⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣿⡇⠀⠘⣧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣸⡏⠀⢸⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⣿⣧⠀⠀⢿⡆⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣿⡇⠀⢸⣿⣿⣿⣿⠀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡇⣿⣿⠀⢠⣼⣇⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⢀⣿⡇⠀⢸⣿⣿⣿⣿⠀⣿⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣷⣿⣿⠀⢸⣿⣿⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⣸⣿⣇⣀⣾⣿⣿⣿⣿⣰⣿⡏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠿⠿⢟⣀⣸⣿⣿⡇⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣶⣶⣶⣶⣶⣶⣶⣶⣶⣶⣶⡶⠶⠶⠶⠶⠖⠒⠒⠒⣾⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠘⣿⣿⣿⣿⠟⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣦⣤⣤⣤⣤⣤⣤⣿⣿⣿⡿⠿⡟⢻⠋⢹⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢸⢻⡏⣿⣿⡄⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠋⠁⣀⣀⣀⡀⠈⠉⠻⣿⣿⣷⣤⣀⡀⣿⣿⣿⠇⢰⠃⣸⢠⣼⣧⡀⠀⠀⠀⠀
⠀⠀⠀⢰⣿⣏⡇⢻⢿⢧⣸⣿⣿⣿⣿⣿⣿⡿⠟⠋⣁⣴⣾⠟⠉⠀⠀⠉⠓⢤⡀⠈⠙⢿⣿⣿⣿⣿⣿⡟⠀⡜⠀⡏⢸⣿⣿⡇⠀⠀⠀⠀
⠀⠀⠀⠸⣿⣿⠏⣾⣮⢎⠻⠿⠟⠛⠛⠉⢁⣠⡴⣿⣿⠟⠁⠀⠀⠀⠀⠀⠀⠀⠈⠳⢤⡀⠈⠙⠛⠛⠋⠀⣼⣿⠀⡇⢘⣿⣿⡀⠀⠀⠀⠀
⠀⠀⠀⠀⠸⣿⡀⣿⣿⣷⣀⣠⠤⠤⠒⠋⢡⢫⣾⣿⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠓⠲⠤⢤⣤⣾⣿⣿⠀⣧⠻⣿⣿⣿⣷⣤⠀⠀
⠀⠀⠀⠀⢠⣿⠇⣿⣿⣿⡿⠁⠀⠀⠀⠀⢀⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣿⡀⠘⢦⡈⠙⢿⣿⣿⣷⡀
⠀⠀⢀⣾⠟⠁⢀⠘⣿⣿⠃⠀⠀⠀⠀⠀⣸⣿⡇⠀⠀⠀⢀⣴⣾⣿⣿⣿⣶⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿⢻⠀⠀⠙⢦⡀⠙⢿⣿⡇
⠀⣰⣿⠃⠀⢀⣾⣧⡈⠟⠀⠀⠀⠀⠀⠀⠘⣿⡁⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⡀⠀⠀⠀⠀⠀⠀⡱⣫⠊⠀⢀⣤⡄⠹⣆⠘⣿⣿
⢰⣿⣧⡖⠼⢟⣻⣿⣿⣦⡀⢤⣀⠀⠀⠀⢠⣿⣿⣿⣿⡿⠿⣛⠛⠛⠛⠛⠛⠿⢿⣿⣿⣿⣦⡀⠀⠀⠀⢎⠞⠁⠀⣠⣾⠟⠀⠀⠘⡄⣿⡏
⣿⣿⡿⠁⠈⠽⢚⣻⣿⣿⣿⣾⣿⠿⣂⣴⣿⣿⣿⣿⣿⣿⣿⠋⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⢿⣿⣦⠀⢠⠏⠀⣠⣾⣿⠏⠀⠀⠀⡇⣇⣿⠃
⢿⣿⡇⡇⠀⠘⣩⠴⠛⣛⣩⣟⣛⠫⠉⠉⠻⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⠈⣛⣀⠐⠻⠿⠃⠀⠀⣰⣿⣿⣿⠏⠀
⠸⣿⣷⡳⣀⢀⣠⣶⣿⡟⠁⠀⠈⠙⠲⣄⠀⠈⢻⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠔⠉⠀⠀⠀⠀⠀⢦⣀⣼⣿⣿⡿⠋⠀⠀
⠀⠹⣿⣿⣮⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠘⣦⠀⠀⢻⣿⣿⠿⠒⠋⣉⠁⠐⢤⡀⠀⠀⠀⠀⣰⠋⠀⠀⠀⠀⠀⠀⠀⢀⢻⣿⣿⠟⠁⠀⠀⠀
⠀⠀⠹⣿⣿⣿⢿⣿⡟⠀⠀⠀⣠⠤⠤⢤⣾⣧⡀⠈⡿⡁⢰⣦⣠⣿⣧⡀⠀⣿⡄⣄⣀⣀⣁⣀⠤⠤⢤⣀⠀⠀⠀⢸⡎⣿⠏⠀⠀⠀⠀⠀
⠀⠀⠀⠙⢿⣿⠀⠻⣿⣆⠀⠞⢡⣴⠶⢤⣍⡛⠿⠛⠀⣷⢸⣿⣿⣿⣏⣷⠈⣽⣷⣌⠉⢉⣡⣴⣶⣶⣤⡈⠱⡄⠀⣼⢱⡿⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠈⠻⣧⡤⠄⣉⡀⢰⣿⡇⡖⠛⢻⣿⡶⠄⠀⣿⣾⣿⣿⣿⣿⢹⣷⣦⠀⠈⣿⢿⠟⠛⢮⣿⡿⣿⡄⢹⠰⢃⣾⠇⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠙⢿⣽⡒⠅⠈⠻⣷⣧⣤⣼⡿⠾⠀⠀⠻⣿⣿⣿⣿⣿⣾⣿⠏⠀⠀⠴⢿⣦⡤⠜⠩⠞⠟⠁⡜⣠⡾⠋⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠈⠙⢦⡀⠀⠀⠀⠈⠀⠀⠀⠀⢀⣠⣾⠿⠿⠿⠿⠿⠿⣿⣦⡀⣀⣀⠀⠀⠀⠀⠀⠀⢀⣠⡾⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠓⠲⠤⠤⣤⣤⠴⠖⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⢭⣍⣀⣀⣠⡤⠶⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"""


# Function to sort columns based on different criteria
def sort_columns(columns, by="name"):
    if by == "name":
        return sorted(columns, key=lambda col: col.get_name())
    elif by == "type":
        return sorted(columns, key=lambda col: col.tokens[2].value.lower())
    else:
        raise ValueError("Unknown sorting criteria")


def _get_permutations_by_type(context: DDLTableContext) -> list[tuple[DDLTableColumn]]:
    # Group columns by their type
    columns_by_type = defaultdict(list)
    for column in context.columns:
        columns_by_type[column.type].append(column)

    # Get all unique types
    unique_types = list(columns_by_type.keys())

    # Generate all permutations of the types
    type_permutations = list(itertools.permutations(unique_types))

    # Generate permutations of columns based on type permutations
    column_permutations = []
    for type_permutation in type_permutations:
        # skip neutral permutation
        if list(type_permutation) == unique_types:
            continue

        perm = []
        for column_type in type_permutation:
            perm.extend(columns_by_type[column_type])
        column_permutations.append(tuple(perm))

    return column_permutations


def compute_permutations(
    context: DDLTableContext, request: PermutationRequest
) -> PermutationSettings:
    """
    Build a mapping of shuffled indexes of the columns and their permutation.
    The permutation is expressed as the sorted list of columns.
    TODO I can just permute the indexes
    """
    permutation_dict = {}
    original_indices = {item: index for index, item in enumerate(context.columns)}

    permutations: Iterable[tuple[DDLTableColumn]]

    if request == PermutationRequest.ALL:
        permutations = itertools.permutations(context.columns)  # type: ignore
    elif request == PermutationRequest.CLUSTER_BY_TYPE:
        permutations = _get_permutations_by_type(context)
    elif request == PermutationRequest.MAGIC:
        raise ValueError("Magic permutations are nto yet implemented")
    else:
        raise ValueError(f"Permutation request {request} not valid")

    for perm in permutations:
        key = "".join(str(original_indices[item]) for item in perm)
        permutation_dict[key] = list(perm)

    return PermutationSettings(context=context, permutation_dict=permutation_dict)


def make_permutation_ddl(
    project_name: str,
    permutation_key: str,
    context: DDLTableContext,
    permutation: list[DDLTableColumn],
) -> str:
    """
    Generate the DDL for the permutation
    """
    columns = []
    for column in permutation:
        columns.append(column.original_definition)

    TEMPLATE = f"""
CREATE TABLE IF NOT EXISTS {project_name}_{permutation_key} (
{",".join(columns)}
);
"""
    return TEMPLATE


def validate_no_permutations_exists_for_project(
    project_name: str, permutations_settings: PermutationSettings
) -> None:
    schema = find_schema_by_name(project_name)
    if schema is None:
        raise ValueError(f"Schema {project_name} not found")

    # Check for existing permutations and avoid re-generating them
    with context_session() as session:
        existing_permutations = {
            p.permutation_code
            for p in session.query(Permutation)
            .filter(
                Permutation.schema_id == schema.id,
                Permutation.permutation_code.in_(
                    permutations_settings.permutation_dict.keys()
                ),
            )
            .all()
        }
        if existing_permutations:
            raise ValueError("Permutations already exists. Use the clean command first")


def save_baseline_permutation(schema: Schema) -> None:
    # Write create statement
    with context_session() as session:
        # Create the object
        session.add(
            Permutation(
                is_permutation=False,
                is_populated=False,
                schema_id=schema.id,
                schema=schema,
                permutation_code=-1,
                name=schema.original_table_name,
            )
        )


def save_permutation(
    permutation_key: str, permutation_ddl: str, schema: Schema
) -> None:
    # Write create statement
    with context_session() as session:
        session.execute(text(f"SET search_path TO {schema.name}"))

        # Execute the DDL
        session.execute(text(permutation_ddl))

        session.execute(text("SET search_path TO public"))

        # Create the object
        session.add(
            Permutation(
                is_permutation=True,
                is_populated=False,
                schema_id=schema.id,
                schema=schema,
                permutation_code=permutation_key,
                name=f"{schema.name}_{permutation_key}",
            )
        )


def generate_permutations_for_project(project_name: str) -> None:
    schema = find_schema_by_name(project_name)
    if schema is None:
        raise ValueError(f"Schema {project_name} not found")

    # Extract the DDL
    ddl = read_ddl_for_project(project_name=project_name)

    # Parse the DDL into a statement object
    create_statement = extract_create_statement(ddl)

    # Get the context, so that we can easily create permutations
    context: DDLTableContext = parse_create_statement(
        project_name=project_name, statement=create_statement
    )

    # Generate the possible permutations
    permutations_settings: PermutationSettings = compute_permutations(
        context, PermutationRequest.CLUSTER_BY_TYPE
    )

    # Should avoid re-generating existing permutations
    validate_no_permutations_exists_for_project(project_name, permutations_settings)

    save_baseline_permutation(schema=schema)
    for permutation_key, permutation in permutations_settings.permutation_dict.items():
        # Generate DDL
        permutation_ddl: str = make_permutation_ddl(
            project_name, permutation_key, context, permutation
        )

        # Write them to the DB and to the permutation table
        save_permutation(permutation_key=permutation_key, permutation_ddl=permutation_ddl, schema=schema)  # type: ignore


def load_permutations(schema: Schema, only_origin: bool) -> list[Permutation]:
    """
    Gives you a list of permutations, for the specific schema.
    It is possible to filter them, by returning only the origin table by passing the flag only_origin.
    """
    with context_session() as session:
        if only_origin:
            # FIXME Should be loading them all and filter later, perhaps.
            return [
                session.query(Permutation)
                .filter(Permutation.schema_id == schema.id)
                .order_by(Permutation.id.asc())
                .one()
            ]
        else:
            return (
                session.query(Permutation)
                .filter(Permutation.schema_id == schema.id)
                .all()
            )
