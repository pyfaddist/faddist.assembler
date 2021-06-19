from faddist.iterators.csv_iterator import CsvIterator
from utils import absolute_path_to


def test_csv_iterate_simple():
    rows = []
    for row in CsvIterator(absolute_path_to('samples', 'data.simple.csv')):
        rows.append(row)

    assert len(rows) == 5

    row1 = rows[0]
    assert row1 is not None
    assert row1 == {'Spalte1': 'Sp1Z1', 'Spalte2': 'Sp2Z1', 'Spalte3': 'Sp3Z1'}

    row5 = rows[4]
    assert row5 is not None
    assert row5 == {'Spalte1': 'Sp1Z5', 'Spalte2': 'Sp2Z5', 'Spalte3': 'Sp3Z5'}
