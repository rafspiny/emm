import logging
from contextlib import contextmanager

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    MappedAsDataclass,
    registry,
    scoped_session,
    sessionmaker,
)

from src.emm.config.config import Config, config

log = logging.getLogger(__name__)


def _build_db_url(config: Config) -> str:
    conn_str: str = (
        f"postgresql://{config.emm_db_user}:{config.emm_db_pwd}@"
        f"{config.emm_db_host}:{config.emm_db_port}/{config.emm_db}"
    )
    log.debug(f"Connection string is {conn_str}")
    return conn_str


engine = create_engine(url=_build_db_url(config=config), future=True)
metadata = MetaData()
mapper_registry = registry()
session_factory = sessionmaker(bind=engine, future=True)
Session = scoped_session(session_factory)


def get_db_session():
    session = Session()

    return session


# Following recommended way from SQLAlchemy to have declarative classes
# that are dataclasses too...
class SQLBase(MappedAsDataclass, DeclarativeBase):
    """
    Subclasses will be converted to dataclasses
    """


# Based on https://gist.github.com/naufalafif/bb2e238f9f80aa17a16ebd7023afb935
@contextmanager
def context_session():
    """
    Yield the session object. It auto-commits and the end if no exceptions
    have been raised.
    """
    session = get_db_session()
    try:
        yield session
    except Exception as e:
        log.exception("Error occurred in the DB session context manager")
        session.rollback()
        raise e
    finally:
        session.commit()
