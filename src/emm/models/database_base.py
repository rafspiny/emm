from typing import Callable, TypeVar
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import registry, Session
from src.emm.config.config import config, Config


MODEL_TYPE = TypeVar("MODEL_TYPE")


def _build_db_url(config: Config) -> str:
    return f"postgres://{config.emm_db_user}:{config.emm_db_pwd}@{config.emm_db_host}:{config.emm_db_port}/{config.emm_db}"

engine = create_engine()(url=_build_db_url(config=config), future=True)
metadata = MetaData(bind=engine)
mapper_registry = registry(metadata)


def get_db_session() -> Session:
    session = Session(bind=engine, future=True)

def emm_model(*, table_name: str, schema: str) -> Callable[[type[MODEL_TYPE]], type[MODEL_TYPE]]:
    """
    A decorator to easily create sqlalchemy models out of dataclasses
    """
    def wrap(cls: type[MODEL_TYPE]) -> type[MODEL_TYPE]:
        cls.__tablename__ = table_name
        cls.__table_args__ = {"schema": schema}

        return mapper_registry.mapped(cls)
    
    return wrap