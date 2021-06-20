from typing import Iterable, Iterator, Union

from sqlalchemy import text
from sqlalchemy.engine import Engine, CursorResult, Row


class CursorResultIterator(Iterator[dict]):
    def __init__(self, cursor_result: CursorResult):
        self.__cursor_result = cursor_result

    def __next__(self) -> dict:
        return self.next()

    def next(self) -> dict:
        try:
            row = next(self.__cursor_result)
            # noinspection PyProtectedMember
            return row._asdict()
        except StopIteration:
            self.__cursor_result.close()
            raise


class DatabaseReader(Iterable[dict]):
    def __init__(self, engine: Engine, statement: Union[str, text], params: dict = None):
        self.__engine = engine
        self.__statement = statement
        self.__params = params

    def __iter__(self) -> Iterator[dict]:
        if self.__params:
            return CursorResultIterator(self.__engine.execute(self.__statement, self.__params))
        return CursorResultIterator(self.__engine.execute(self.__statement))
