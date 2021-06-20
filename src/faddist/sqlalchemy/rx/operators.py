import traceback
from typing import Any, Callable

from pydash import get, set_
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker, scoped_session

from faddist.iterators import DatabaseReader
from faddist.rx import subscribe


def raise_runtimeerror() -> Session:
    raise RuntimeError('Session factory is not initialized yet.')


get_current_session: Callable[[], Session] = raise_runtimeerror


@subscribe
def session(engine):
    global get_current_session
    get_current_session = scoped_session(sessionmaker(bind=engine))
    get_current_session()

    def on_next(data: Any) -> Any:
        return data

    @session.on_completed
    def on_completed():
        get_current_session.remove()

    return on_next


@subscribe
def select_first(engine: Engine, source: str, target: str, table_name: str):
    parts = source.split('.')
    if len(parts) == 0:
        raise ValueError(f"Invalid source '{source}'")
    key = parts[-1:][0]
    statement = text(f"SELECT * FROM '{table_name}' WHERE {key} = :{key}")

    def on_next(data: dict) -> dict:
        try:
            result = data.copy()
            value = get(data, source)
            params = {key: value}
            try:
                row = next(engine.execute(statement, params))
                if row:
                    # noinspection PyProtectedMember
                    set_(result, target, row._asdict())
            except StopIteration:
                pass
            return result
        # noinspection PyBroadException
        except Exception:
            traceback.print_exc()
            raise

    return on_next


@subscribe
def select_all(engine: Engine, source: str, target: str, table_name: str):
    parts = source.split('.')
    if len(parts) == 0:
        raise ValueError(f"Invalid source '{source}'")
    key = parts[-1:][0]
    statement = text(f"SELECT * FROM '{table_name}' WHERE {key} = :{key}")

    def on_next(data: dict) -> dict:
        try:
            result = data.copy()
            value = get(data, source)
            params = {key: value}
            set_(result, target, [entry for entry in DatabaseReader(engine, statement, params)])
            return result
        # noinspection PyBroadException
        except Exception:
            traceback.print_exc()
            raise

    return on_next
