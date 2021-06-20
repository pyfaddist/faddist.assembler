import datetime
from typing import Callable, Any

from rx import from_
from sqlalchemy.engine import Engine

from faddist.assembler import Assembler
from faddist.rx.operators import buffer_while


def test_buffer_while():
    assertion_data = [
        (2, [{'g': 1, 'v': 1}, {'g': 1, 'v': 2}, ]),
        (1, [{'g': 2, 'v': 3}, ]),
        (1, [{'g': 1, 'v': 4}, ]),
        (3, [{'g': 2, 'v': 5}, {'g': 2, 'v': 6}, {'g': 2, 'v': 7}, ]),
        (1, [{'g': 1, 'v': 8}, ])
    ]
    assertion_index = 0

    def assert_subscription(buffer):
        nonlocal assertion_index
        assertion_entry = assertion_data[assertion_index]
        assertion_index += 1
        assert len(buffer) == assertion_entry[0], f"expect {buffer} to have {assertion_entry} elements"
        assert buffer == assertion_entry[1]

    def buffering_condition():
        last_g = None

        def wrapper(data: dict):
            nonlocal last_g
            g = data.get('g')
            result = not last_g or last_g == g
            last_g = g
            return result

        return wrapper

    source = from_([
        {'g': 1, 'v': 1},
        {'g': 1, 'v': 2},
        {'g': 2, 'v': 3},
        {'g': 1, 'v': 4},
        {'g': 2, 'v': 5},
        {'g': 2, 'v': 6},
        {'g': 2, 'v': 7},
        {'g': 1, 'v': 8},
    ]).pipe(
        buffer_while(buffering_condition())
    )
    source.subscribe(assert_subscription)


def sample_transform_callable(source: str, formats: list[str], default: Any = None) -> Callable[[Any], Any]:
    def convert(data: Any):
        for format_ in formats:
            try:
                return datetime.datetime.strptime(data.get(source), format_).date()
            except ValueError:
                pass
        return default

    return convert


def test_transform():
    assembler = Assembler()

    pipeline = assembler.build_pipeline({'alias': [{'__type__': 'datetime.date', 'name': 'Date'},
                                                   {'__type__': 'datetime.datetime', 'name': 'DateTime'},
                                                   {'__type__': 'faddist.rx.operators.TransformBuilder',
                                                    'name': 'transform'},
                                                   {'__type__': 'test_rx_operators.sample_transform_callable',
                                                    'name': 'parse_date'}],
                                         'variables': [{'__type__': 'list', 'name': 'test'},
                                                       {'__type__': 'int', 'name': 'start', 'arguments': '10'},
                                                       {'__type__': 'int', 'name': 'stop', 'arguments': '20'}],
                                         'iterator': {'__type__': 'range', 'arguments': ['$var:start', '$var:stop']},
                                         'pipe': [
                                             {'__type__': 'rx.operators.map',
                                              'arguments': ['$lambda x: {"a": "2021-05-" + str(x), "x": x}']},
                                             {'__alias__': 'transform',
                                              'arguments': [{
                                                  'origin': 'x',
                                                  'date': '$lambda x: DateTime.strptime(x["a"], "%Y-%m-%d").date()',
                                                  'other': {
                                                      '__alias__': 'parse_date',
                                                      'arguments': ['a', ['%m/%d%/%Y', '%Y-%m-%d']]
                                                  }
                                              }]}
                                         ],
                                         'observer': '$lambda x: test.append(x)'})
    assert pipeline is not None
    assert assembler.has_variable('test')

    pipeline.operate()

    data = assembler.get_variable('test')
    assert len(data) == 10

    entry = data[0]
    assert 'date' in entry
    assert entry['date'] == datetime.date(2021, 5, 10)
    assert 'other' in entry
    assert entry['other'] == datetime.date(2021, 5, 10)
    assert 'origin' in entry
    assert entry['origin'] == 10

    entry = data[9]
    assert 'date' in entry
    assert entry['date'] == datetime.date(2021, 5, 19)
    assert 'other' in entry
    assert entry['other'] == datetime.date(2021, 5, 19)
    assert 'origin' in entry
    assert entry['origin'] == 19


def test_select_single_northwind(northwind: Engine, db_path: str):
    assembler = Assembler()
    pipeline = assembler.build_pipeline({'alias': [{'__type__': 'faddist.sqlalchemy.rx.operators.select_first',
                                                    'name': 'select_first'},
                                                   {'__type__': 'faddist.sqlalchemy.rx.operators.select_all',
                                                    'name': 'select_all'},
                                                   {"__type__": "faddist.iterators.DatabaseReader",
                                                    "name": "db_reader", }],
                                         'variables': [{'__type__': 'list', 'name': 'test'},
                                                       {"__type__": "sqlalchemy.create_engine",
                                                        "name": "engine",
                                                        "arguments": f"sqlite:///{db_path}"}],
                                         'iterator': {
                                             "__alias__": "db_reader",
                                             "arguments": [
                                                 "$var:engine",
                                                 "SELECT * FROM Orders"
                                             ]
                                         },
                                         'pipe': [
                                             {"__alias__": "select_first",
                                              #              engine         source        target      table_name
                                              "arguments": ["$var:engine", "EmployeeID", "Employee", "Employees"]},
                                             {"__alias__": "select_first",
                                              #              engine         source                target
                                              "arguments": ["$var:engine", "Employee.ReportsTo", "Employee.ReportsTo",
                                                            # table_name
                                                            "Employees"]},
                                         ],
                                         'observer': '$lambda x: test.append(x)'})
    pipeline.operate()
    data = assembler.get_variable('test')
    assert len(data) == 830

    entry_count = 0
    for entry in data:
        assert 'EmployeeID' in entry
        assert 'Employee' in entry
        assert entry['EmployeeID'] == entry['Employee']['EmployeeID']
        entry_count += 1
    assert entry_count == 830


def test_select_all_northwind(northwind: Engine, db_path: str):
    assembler = Assembler()
    pipeline = assembler.build_pipeline({'alias': [{'__type__': 'faddist.sqlalchemy.rx.operators.select_first',
                                                    'name': 'select_first'},
                                                   {'__type__': 'faddist.sqlalchemy.rx.operators.select_all',
                                                    'name': 'select_all'},
                                                   {"__type__": "faddist.iterators.DatabaseReader",
                                                    "name": "db_reader", }],
                                         'variables': [{'__type__': 'list', 'name': 'test'},
                                                       {"__type__": "sqlalchemy.create_engine",
                                                        "name": "engine",
                                                        "arguments": f"sqlite:///{db_path}"}],
                                         'iterator': {
                                             "__alias__": "db_reader",
                                             "arguments": [
                                                 "$var:engine",
                                                 "SELECT * FROM Orders"
                                             ]
                                         },
                                         'pipe': [
                                             {"__alias__": "select_all",
                                              #              engine         source     target     table_name
                                              "arguments": ["$var:engine", "OrderID", "Details", "Order Details"]}
                                         ],
                                         'observer': '$lambda x: test.append(x)'})
    pipeline.operate()
    data = assembler.get_variable('test')
    assert len(data) == 830

    entry_count = 0
    for entry in data:
        assert 'OrderID' in entry
        assert 'Details' in entry
        for detail in entry['Details']:
            assert detail['OrderID'] == entry['OrderID']
        entry_count += 1
    assert entry_count == 830
