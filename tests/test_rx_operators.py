import datetime

from rx import from_

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


def test_rx_transform():
    assembler = Assembler()

    pipeline = assembler.build_pipeline({'alias': [{'__type__': 'datetime.date', 'name': 'Date'},
                                                   {'__type__': 'datetime.datetime', 'name': 'DateTime'},
                                                   {'__type__': 'faddist.rx.operators.TransformBuilder',
                                                    'name': 'transform'}],
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
                                                  'date': '$lambda x: DateTime.strptime(x["a"], "%Y-%m-%d").date()'
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
    assert 'origin' in entry
    assert entry['origin'] == 10

    entry = data[9]
    assert 'date' in entry
    assert entry['date'] == datetime.date(2021, 5, 19)
    assert 'origin' in entry
    assert entry['origin'] == 19
