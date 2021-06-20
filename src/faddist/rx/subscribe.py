from typing import Any, Callable

from rx import Observable
from rx.core.typing import Observer
from rx.scheduler.scheduler import Scheduler


def subscribe(origin_function):
    return SubscribeDecorator(origin_function)


class SubscriptionHandler(object):
    def __init__(self, on_next, on_error, on_completed):
        self.__registered_next = on_next
        self.__registered_error = on_error
        self.__registered_completed = on_completed
        self.__observable: Observable = None
        self.__observer: Observer = None

    def __call__(self, observable: Observable):
        self.__observable = observable
        return Observable(self.__subscribe)

    def __subscribe(self, observer: Observer, scheduler: Scheduler = None):
        self.__observer = observer
        return self.__observable.subscribe(on_next=self.__on_next, on_completed=self.__on_complete,
                                           on_error=self.__registered_error, scheduler=scheduler)

    def __on_next(self, data: Any):
        if self.__registered_next:
            forward = self.__registered_next(data)
        else:
            forward = data
        if forward:
            self.__observer.on_next(forward)

    def __on_error(self, error):
        if self.__registered_error:
            self.__registered_error(error)
        self.__observer.on_error(error)

    def __on_complete(self):
        if self.__registered_completed:
            forward = self.__registered_completed()
            if forward:
                self.__observer.on_next(forward)
        self.__observer.on_completed()


class SubscribeDecorator(object):
    def __init__(self, origin_function):
        self.__origin_function = origin_function
        self.__registered_error = None
        self.__registered_completed = None

    def on_error(self, origin_function: Callable):
        self.__registered_error = origin_function

    def on_completed(self, origin_function: Callable):
        self.__registered_completed = origin_function

    def __call__(self, *args, **kwargs):
        return SubscriptionHandler(self.__origin_function(*args, **kwargs), self.__registered_error,
                                   self.__registered_completed)
