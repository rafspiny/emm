import dataclasses
from datetime import datetime

from src.emm.models.database_base import emm_model


@emm_model(table_name="emm_schema", schema="public")
@dataclasses.dataclass(repr=False)
class Schema:
    id: int
    name: str
    is_populated: bool
    is_permutation: bool
    permutation_id: int
    created: datetime
