from collections import Callable
from typing import Any

from pydash import set_, get

from rx.core.typing import Observable
from rx.operators import map as map_

from faddist.assembler import OperatorBuilder
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


def _create_getter(source):
    def wrapper(input_: Any):
        return get(input_, source)
    return wrapper


class TransformBuilder(OperatorBuilder):
    def __init__(self, mapping: dict, use_origin: bool = False):
        super(TransformBuilder, self).__init__()
        self.__mapping = mapping
        self.__use_origin = use_origin
        self.__processor = []

    def __process_mapping(self, data: Any) -> Any:
        if self.__use_origin:
            result = data.copy()
        else:
            result = {}
        for target, source in self.__processor.items():
            set_(result, target, source(data))
        return result

    def build(self) -> Callable[[Observable], Observable]:
        if not self.__mapping:
            raise ResourceWarning('Missing mapping for map operator')
        processor = {}
        for target, source in self.__mapping.items():
            if isinstance(source, str):
                if source.startswith('$lambda'):
                    processor[target] = self.assembler.instance_from_descriptor(source)
                else:
                    processor[target] = _create_getter(source)
            else:
                processor[target] = self.assembler.instance_from_descriptor(source)
        self.__processor = processor
        return map_(self.__process_mapping)
