from typing import Any, Callable

from rx import Observable
from rx.core.typing import Observer
from rx.scheduler.scheduler import Scheduler


# noinspection PyPep8Naming
class subscribe(object):
    def __init__(self, origin_function):
        self.__origin_function = origin_function
        self.__observable: Observable = None
        self.__observer: Observer = None
        self.__registered_next = None
        self.__registered_completed = None

    def __call__(self, *args, **kwargs):
        self.__registered_next = self.__origin_function(*args, **kwargs)
        return self.__accept

    def completed(self, origin_function: Callable):
        self.__registered_completed = origin_function

    def __accept(self, observable: Observable):
        self.__observable = observable
        return Observable(self.__subscribe)

    def __subscribe(self, observer: Observer, scheduler: Scheduler = None):
        self.__observer = observer
        return self.__observable.subscribe(on_next=self.__on_next, on_completed=self.__on_complete,
                                           on_error=observer.on_error, scheduler=scheduler)

    def __on_next(self, data: Any):
        if self.__registered_next:
            forward = self.__registered_next(data)
        else:
            forward = data
        if forward:
            self.__observer.on_next(forward)

    def __on_complete(self):
        if self.__registered_completed:
            forward = self.__registered_completed()
            if forward:
                self.__observer.on_next(forward)
        self.__observer.on_completed()
