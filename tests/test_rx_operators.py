from rx import from_

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
