import logging
from functools import partial, wraps

import click
from tabulate import tabulate

from src.emm.engine.data import BenchmarkRequest, PermutationRequest
from src.emm.models.schema import Schema
from src.emm.operations.perfomances import benchmark_schema, load_analysis_for_schema
from src.emm.operations.permutations import generate_permutations_for_project
from src.emm.operations.population import populate_schema
from src.emm.operations.schemas import (
    delete_schema,
    find_schema_by_name,
    initialize_schema,
    load_schemas,
)
from src.emm.operations.validators import validate_sql_folder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.NOTSET)


def catch_exception(func=None, *, handle):
    if not func:
        return partial(catch_exception, handle=handle)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except handle as e:
            raise click.ClickException(e)

    return wrapper


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
@catch_exception(handle=Exception)
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
    "--permutation-logic",
    default=None,
    help="Type of permutation to compute. Possible options are: all, type. Defaults to all",
)
@catch_exception(handle=Exception)
def permutations(schema_name: str, permutation_logic: str | None) -> None:
    """
    Init the schema based on the file sql/init/*.sql
    """
    if not validate_sql_folder(sql_folder_path=schema_name):
        raise Exception("Project folder not found or has invalid structure")

    permutation_request = get_permutation_request_from_argument(permutation_logic)

    generate_permutations_for_project(
        project_name=schema_name, permutation_request=permutation_request
    )
    click.echo("Permutations generated")


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
    else:
        click.echo(f"Schema {schema_name} not found")


@cli.command(name="benchmark")
@click.option("--schema-name", default=None, help="Schema name to run benchmark on")
@click.option(
    "--benchmark-logic",
    default=None,
    help="The kind of benchamrk to run. Possible options are: all, size. Defaults to all",
)
@catch_exception(handle=Exception)
def benchmark_schemas(schema_name: str, benchmark_logic: str) -> None:
    """
    Run benchmarks
    """
    schema: Schema | None = find_schema_by_name(schema_name)
    benchmark_request = get_benchmark_request_from_argument(benchmark_logic)

    if schema:
        benchmark_schema(schema, benchmark_request)
        click.echo(f"Schema {schema_name} benchmark finished.")
    else:
        click.echo(f"Schema {schema_name} not found")


@cli.command(name="report")
@click.option(
    "--schema-name",
    default=None,
    help="Schema name for which the report(s) should be printed",
)
def print_schema_analysis(schema_name: str) -> None:
    """
    Load the analysis for the specified schema and print them
    """
    click.echo("Loading schema analysis")

    schema = find_schema_by_name(schema_name)
    if schema is None:
        click.echo(f"Schema {schema_name} not found")
        return

    for analysis in load_analysis_for_schema(schema):
        click.echo(f"Analysis {analysis.name}")
        # Prepare data for the Markdown table
        table_data = [
            [
                report.metric,
                report.best_permutation_name,
                f"{report.improvement_percentage_over_baseline:.2f}%",
                report.original_metric_value,
                report.permutation_metric_value,
            ]
            for report in analysis.reports
        ]
        headers = [
            "Metric",
            "Best Permutation",
            "Improvement (%)",
            "Baseline value",
            "Permutation value",
        ]

        # Print Markdown table using tabulate
        markdown_table = tabulate(
            table_data, headers=headers, tablefmt="github", maxcolwidths=25
        )
        click.echo(markdown_table)
        click.echo("\n")


def get_permutation_request_from_argument(
    permutation_logic: str | None,
) -> PermutationRequest:
    if permutation_logic is None:
        return PermutationRequest.ALL

    try:
        return PermutationRequest(permutation_logic.lower())
    except ValueError:
        log.info(f"Value {permutation_logic} not valid. Defaults to all.")
        return PermutationRequest.ALL


def get_benchmark_request_from_argument(
    benchmark_logic: str | None,
) -> BenchmarkRequest:
    if benchmark_logic is None:
        return BenchmarkRequest.ALL

    try:
        return BenchmarkRequest(benchmark_logic.lower())
    except ValueError:
        log.info(f"Value {benchmark_logic} not valid. Defaults to all.")
        return BenchmarkRequest.ALL
