from _csv import get_dialect
from csv import DictReader
from typing import Iterable, Iterator


class CsvIterator(Iterable[dict], Iterator[dict]):
    def __init__(self, file_path: str, dialect_name: str):
        if dialect_name:
            dialect = get_dialect(dialect_name)
        else:
            dialect = None
        self.__file_handle = open(file_path, 'r')
        self.__reader = DictReader(self.__file_handle, dialect=dialect)
        self.__fieldnames = self.__reader.fieldnames

    @property
    def fieldnames(self):
        return self.__fieldnames

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.__reader)
        except StopIteration:
            self.__file_handle.close()
            raise
