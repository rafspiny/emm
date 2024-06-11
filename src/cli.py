from os import environ
import click
from src.emm.operations.schemas import load_schemas
from src.emm.config.config import Config


@click.group()
@click.version_option()
def cli():
    "EMM is designed to help you find a byte-alignment-optimized columns order for your table.\
    It manages different projects and it helps you run performance tests to understand the impact of byte alignment on the table."


@cli.command(name="ls")
def list_schemas() -> None:
    """
    List all the schemas present in the DB.
    """
    print("SCHEMAS")
    for schema in fetch_schemas():
        click.echo(f"Found schema: {schema.name}")


@cli.command(name="clean")
@click.option("--dry-run", default=True, help="Only shows what it is going to be done")
@click.option("--keep-original", default=False, help="Force to keep the originla schema.")
def clean_schemas(dry_run: bool, keep_original: bool) -> None:
    """
    Remove all the schemas from the DB.
    """
    load_schemas()
    click.echo(f"Removed schema")


@cli.command(name="init")
# @click.option("--sql-path", default=None, help="SQL file path for table initialization")
def init_schema() -> None:
    """
    Init the schema based on the file sql/init/*.sql
    """
    click.echo(f"Schema initialized")


@cli.command(name="populate")
# @click.option("--sql-path", default=None, help="SQL file path containing the data to use to populate the table")
def insert_into_schema() -> None:
    """
    Insert data from file sql/data/*.sql into the table
    """
    click.echo(f"Schema populated")


@cli.command(name="perms")
# @click.option("--sql-path", default=None, help="SQL file path containing the data to use to populate the table")
def generate_permutation_schemas() -> None:
    """
    Generates permutations based on the table defined in your project
    """
    click.echo(f"Permutation schemas generated")


@cli.command(name="benchmark")
def benchmark_schemas() -> None:
    """
    Run benchmarks
    """
    click.echo(f"Benchmark executed")


@cli.command(name="env")
def print_env() -> None:
    """
    Print env variables
    """
    click.echo(f"Loading env")
    
    config = Config()
    for key, value in config.__dict__.items():
        print(f"conf {key}: {value}")