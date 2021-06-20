import os
from tempfile import mkstemp

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from utils import absolute_path_to


def _get_temp_filepath(prefix: str, suffix:str):
    fd, filepath = mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    return filepath


@pytest.fixture()
def db_path():
    db_path = _get_temp_filepath(prefix='faddist_', suffix='.db')
    try:
        yield db_path
    finally:
        os.unlink(db_path)
        pass


@pytest.fixture()
def engine(db_path: str):
    return create_engine(f"sqlite:///{db_path}")


@pytest.fixture()
def northwind(engine: Engine):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    with open(absolute_path_to('./data/northwind.sql'), 'r') as fd:
        cursor.executescript(fd.read())
    conn.commit()
    return engine
