import logging

import click

from src.emm.config.config import Config
from src.emm.models.schema import Schema
from src.emm.operations.schemas import (
    delete_schema,
    find_schema_by_name,
    initialize_schema,
    load_schemas,
    populate_schema,
)
from src.emm.operations.validators import validate_sql_folder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.NOTSET)


@click.group()
@click.version_option()
def cli():
    "EMM is designed to help you find a byte-alignment-optimized columns order"
    "for your table. It manages different projects and it helps you run performance "
    "tests to understand the impact of byte alignment on the table."


@cli.command(name="ls")
def list_schemas() -> None:
    """
    List all the schemas present in the DB.
    """
    for schema in load_schemas():
        click.echo(f"Found schema: {schema.name}")


@cli.command(name="clean")
@click.option("--dry-run", default=True, help="Only shows what it is going to be done")
@click.option(
    "--keep-original", default=False, help="Force to keep the originla schema."
)
def clean_schemas(dry_run: bool, keep_original: bool) -> None:
    """
    Remove all the schemas from the DB.
    """
    schemas: list[Schema] = load_schemas()
    for schema in schemas:
        click.echo(f"Going to remove schema {schema.name}. Dry run is {str(dry_run)}")
        if not dry_run or (keep_original and not schema.is_permutation):
            delete_schema(schema)
            click.echo(f"Removed schema {schema.name}")

    if dry_run:
        click.echo("No schema was removed. dry-run is True")
    else:
        click.echo("Removed all schemas")


@cli.command(name="init")
@click.option(
    "--sql-folder-path",
    default=None,
    help="SQL file path for initialization",
    required=True,
)
def init_schema(sql_folder_path: str) -> None:
    """
    Init the schema based on the file sql/init/*.sql
    """
    if not validate_sql_folder(sql_folder_path):
        raise Exception("Project folder not found or has invalid structure")

    initialize_schema(sql_folder_path)
    click.echo("Schema initialized")


@cli.command(name="permutations")
@click.option(
    "--schema-name",
    default=None,
    required=True,
    help="Name of the schema for which permutations have to be generated",
)
@click.option(
    "--override-table-name",
    default=None,
    help="Name of the table to parse in order to generate the permutation",
)
def permutations(sql_folder_path: str, override_table_name: str | None) -> None:
    """
    Init the schema based on the file sql/init/*.sql
    """
    if not validate_sql_folder(sql_folder_path):
        raise Exception("Project folder not found or has invalid structure")

    initialize_schema(sql_folder_path)
    click.echo("Schema initialized")


@cli.command(name="populate")
@click.option("--schema-name", default=None, help="Schema name to populate")
@click.option("--only-original", default=False, help="Schema name to populate")
def insert_into_schema(schema_name: str, only_original: bool) -> None:
    """
    Insert data from file sql/data/*.sql into the table
    """
    schema: Schema | None = find_schema_by_name(schema_name)
    if schema:
        populate_schema(schema, only_original)

    click.echo("Schema populated")


@cli.command(name="perms")
# @click.option("--sql-path", default=None, help="SQL file path to populate the table")
def generate_permutation_schemas() -> None:
    """
    Generates permutations based on the table defined in your project
    """
    click.echo("Permutation schemas generated")


@cli.command(name="benchmark")
def benchmark_schemas() -> None:
    """
    Run benchmarks
    """
    click.echo("Benchmark executed")


@cli.command(name="env")
def print_env() -> None:
    """
    Print env variables
    """
    click.echo("Loading env")

    config = Config()
    for key, value in config.__dict__.items():
        print(f"conf {key}: {value}")
