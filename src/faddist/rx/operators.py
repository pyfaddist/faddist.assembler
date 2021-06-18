from collections import Callable
from typing import Any

from faddist.rx import subscribe


@subscribe
def buffer_while(condition: Callable[[Any], bool]):
    buffer = []

    def on_next(data):
        nonlocal buffer
        if not condition(data):
            result = buffer
            buffer = [data]
            return result
        else:
            buffer.append(data)

    @buffer_while.on_completed
    def on_completed():
        nonlocal buffer
        if len(buffer) >= 0:
            result = buffer
            buffer = []
            return result

    return on_next
