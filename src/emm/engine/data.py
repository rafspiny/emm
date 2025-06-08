from enum import Enum

from sqlparse.tokens import Token


class ParsingContext:
    tokens: list[Token] = []
    in_create: bool = False
    in_create_table: bool = False
    in_columns: bool = False
    current_column_definition: list[str] = []

    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens


class DDLTableColumn:
    """
    A small data structure carrying the name of a column and the original string representation of its definition.
    """

    name: str
    type: str
    original_definition: str

    def __init__(self, name: str, column_type: str, original_definition: str) -> None:
        self.name = name
        self.type = column_type
        self.original_definition = original_definition


class DDLTableContext:
    """
    Context for DDL statements
    """

    project_name: str
    _table_name: str
    _columns: list[DDLTableColumn]

    def __init__(self, project_name: str) -> None:
        self.project_name = project_name
        self._table_name = ""
        self._columns = []

    @property
    def table_name(self):
        """The table name"""
        return self._table_name

    @table_name.setter
    def table_name(self, table_name: str):
        self._table_name = table_name

    @property
    def columns(self):
        return self._columns

    def add_column(self, column_identifier: DDLTableColumn) -> None:
        self._columns.append(column_identifier)


class PermutationRequest(Enum):
    """
    Contains information needed, and concerning, how to generate the permutations
    """

    ALL = "all"
    CLUSTER_BY_TYPE = "type"
    MAGIC = "magic"


class PermutationSettings:
    """
    Data structure to keep the permutations of the columns
    """

    context: DDLTableContext
    permutation_dict: dict[str, list[DDLTableColumn]]

    def __init__(
        self,
        context: DDLTableContext,
        permutation_dict: dict[str, list[DDLTableColumn]],
    ) -> None:
        self.context = context
        self.permutation_dict = permutation_dict


class BenchmarkRequest(Enum):
    """
    Specify what kind of benchmark to run
    """

    ALL = "all"
    TABLE_SIZE = "size"
    FLASK_RO = "ro"
    FLASK_RW = "rw"
    FLASK_MIX = "rw_ro_mix"


class ReadOnlyWorkloadType(Enum):
    """
    Specify the type of query the workload is gonna use.
    Examples of queries are:
    # READ_ALL
    "SELECT * FROM original_table",

    # READ_PRIMARY_KEY_FILTER
    "SELECT * FROM original_table WHERE id < 100;",

    # READ_AGGREGATION
    "SELECT COUNT(*) FROM original_table;",

    # READ_AGGREGATION_FILTER
    "SELECT COUNT(*) FROM original_table WHERE id < 100;",

    # READ_LIKE
    "SELECT name FROM original_table WHERE original_table_name LIKE 'original_schema_name_value%';",

    # READ_ORDER_BY
    "SELECT id, name FROM original_table ORDER BY created DESC LIMIT 50;",

    # READ_RANGE_FILTER
    "SELECT * FROM original_table WHERE id BETWEEN 500 AND 1000;",

    # READ_PAGINATION
    "SELECT * FROM original_table LIMIT 100 OFFSET 200;",
    """

    READ_ALL = "read_all"
    READ_PRIMARY_KEY_FILTER = "read_primary_key_filter"
    READ_AGGREGATION = "read_aggregation"
    READ_AGGREGATION_FILTER = "read_aggregation_filter"
    READ_LIKE = "read_like"
    READ_ORDER_BY = "read_order_by"
    READ_RANGE_FILTER = "read_range_filter"
    READ_PAGINATION = "read_pagination"
