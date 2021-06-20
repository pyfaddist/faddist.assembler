import json
import base64
from datetime import datetime, date

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from faddist.iterators.database_iterator import DatabaseReader


def test_iterator(engine: Engine):
    with Session(engine) as session:
        session.begin()
        session.execute('CREATE TABLE IF NOT EXISTS test ('
                        'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                        'value INTEGER NOT NULL, '
                        'doubled INTEGER NOT NULL'
                        ')')
        session.commit()

        statement = text('INSERT INTO test (value, doubled) VALUES (:value, :doubled)')
        for i in range(1000, 2000):
            session.execute(statement, {'value': i, 'doubled': i * 2})
        session.commit()

    data = []
    for entry in DatabaseReader(engine, 'SELECT * FROM test'):
        data.append(entry)

    assert len(data) == 1000

    entry1 = data[0]
    assert 'id' in entry1
    assert 'value' in entry1
    assert 'doubled' in entry1
    assert entry1 == {'id': 1, 'value': 1000, 'doubled': 2000}

    entry1000 = data[999]
    assert 'id' in entry1000
    assert 'value' in entry1000
    assert 'doubled' in entry1000
    assert entry1000 == {'id': 1000, 'value': 1999, 'doubled': 3998}
