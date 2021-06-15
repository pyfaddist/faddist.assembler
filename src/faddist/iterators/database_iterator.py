from typing import Iterable, Iterator

from sqlalchemy.engine import Engine, CursorResult, Row


class CursorResultIterator(Iterator[dict]):
    def __init__(self, cursor_result: CursorResult):
        self.__cursor_result = cursor_result

    def __next__(self) -> dict:
        return self.next()

    def next(self) -> dict:
        try:
            row = next(self.__cursor_result)
            return row._asdict()
        except StopIteration:
            self.__cursor_result.close()
            raise


class DatabaseReader(Iterable[dict]):
    def __init__(self, engine: Engine, statement: str):
        self.__engine = engine
        self.__statement = statement

    def __iter__(self) -> Iterator[dict]:
        return CursorResultIterator(self.__engine.execute(self.__statement))
