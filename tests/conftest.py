import os
from tempfile import mkstemp

import pytest
from sqlalchemy import create_engine


def _get_temp_filepath(prefix: str, suffix:str):
    fd, filepath = mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    return filepath


@pytest.fixture()
def engine():
    db_path = _get_temp_filepath(prefix='faddist_', suffix='.db')
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        yield engine
    finally:
        os.unlink(db_path)
        pass
