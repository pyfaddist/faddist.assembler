import json
from abc import abstractmethod
from collections import Iterator, Iterable
from logging import Logger
from typing import Union, Any, Callable

from rx import Observable, from_
from rx.core.abc import Observer

from faddist.reflection import load_class, create_instance

_logger = Logger(__file__)


class Pipeline(object):
    def __init__(self, iterator: Iterator, observer: Observer = None):
        self.__iterator = iterator
        self.__observer = observer
        self.__ducts = []

    @property
    def iterator(self) -> Iterator:
        return self.__iterator

    @property
    def observer(self) -> Observer:
        return self.__observer

    def append(self, duct: Callable[[Any], Any]):
        self.__ducts.append(duct)

    def operate(self, callable_: Callable[[Any], Any] = None):
        observable: Observable = from_(self.__iterator).pipe(*self.__ducts)
        if isinstance(self.__observer, InitializingObserver):
            self.__observer.initialize(self)
        observable.subscribe(self.__observer)
        if callable_ is not None:
            observable.subscribe(callable_)


class InitializingObserver(Observer):
    def __init__(self) -> None:
        super(InitializingObserver, self).__init__()
        self.__has_error = False

    def initialize(self, pipeline: Pipeline):
        _logger.info('Pipeline is initialized.')

    @abstractmethod
    def on_next(self, value: Any) -> None:
        pass

    def on_error(self, error):
        self.__has_error = True
        _logger.error(error, exc_info=True)

    def on_completed(self):
        if not self.__has_error:
            _logger.info('Successfully finished.')
        else:
            _logger.warning('Ended with errors.')


class Assembler(object):
    def __init__(self):
        self.__named_classes = {}
        self.__variables = {}

    def __resolve_arguments(self, descriptor: dict):
        if 'arguments' in descriptor:
            arguments = descriptor['arguments']
            result = []
            if isinstance(arguments, str):
                arguments = [arguments]
            if isinstance(arguments, (Iterable, Iterator, list, tuple)):
                for value in arguments:
                    if isinstance(value, str) and value.startswith('$var:'):
                        variable_name = value[5:]
                        result.append(self.get_variable(variable_name))
                    elif isinstance(value, str) and value.startswith('$lambda'):
                        try:
                            script = self.__create_lambda(value)
                        except:
                            raise SyntaxWarning(f"Check the code of the descriptor '{json.dumps(descriptor)}'.")
                        result.append(script)
                    else:
                        result.append(value)
            return result
        return []

    def __create_lambda(self, value: str):
        return eval(value[1:], self.__variables)

    def __bootstrap_alias(self, definitions: list[dict]):
        if isinstance(definitions, (Iterable, Iterator, list, tuple)):
            for descriptor in definitions:
                if 'name' not in descriptor:
                    raise ResourceWarning('An alias descriptor needs a name definition.')
                if '__type__' not in descriptor:
                    raise ResourceWarning('An alias descriptor needs a __type__ definition.')
                name = descriptor['name']
                type_ = descriptor['__type__']
                self.__named_classes[name] = load_class(type_)

    def __instance_from_descriptor(self, descriptor: dict) -> Any:
        if isinstance(descriptor, str) and descriptor.startswith('$lambda'):
            return self.__create_lambda(descriptor)
        if '__type__' not in descriptor and '__alias__' not in descriptor:
            raise ResourceWarning('An instance descriptor needs a __type__ or __alias__ definition.')
        type_ = descriptor.get('__type__')
        alias = descriptor.get('__alias__')
        arguments = self.__resolve_arguments(descriptor)
        if type_:
            clazz = load_class(type_)
        elif alias:
            clazz = self.get_class(alias)
        else:
            raise ResourceWarning('An instance descriptor needs a __type__ or __alias__ definition.')
        return create_instance(clazz, arguments)

    def __bootstrapp_variables(self, definitions: list[dict]):
        if isinstance(definitions, (Iterable, Iterator, list, tuple)):
            for descriptor in definitions:
                if 'name' not in descriptor:
                    raise ResourceWarning('A variable descriptor needs a name definition.')
                name = descriptor['name']
                self.__variables[name] = self.__instance_from_descriptor(descriptor)

    def bootstrap(self, definitions: dict):
        if 'alias' in definitions:
            self.__bootstrap_alias(definitions['alias'])
        if 'variables' in definitions:
            self.__bootstrapp_variables(definitions['variables'])

    def has_class(self, classname: str):
        return classname in self.__named_classes

    def get_class(self, alias: str):
        return self.__named_classes[alias]

    def new_instance(self, alias: str, arguments: Union[dict, list]):
        clazz = self.get_class(alias)
        return create_instance(clazz, arguments)

    def has_variable(self, name: str):
        return name in self.__variables

    def get_variable(self, name: str):
        return self.__variables[name]

    def build_pipeline(self, definitions: dict) -> Pipeline:
        self.bootstrap(definitions)
        if 'iterator' not in definitions:
            raise ResourceWarning('A variable descriptor needs a name definition.')
        iterator = self.__instance_from_descriptor(definitions['iterator'])
        observer = None
        if 'observer' in definitions:
            observer = self.__instance_from_descriptor(definitions['observer'])
        pipeline = Pipeline(iterator, observer)
        if 'pipe' in definitions:
            pipe_definitions = definitions['pipe']
            for operator_descriptor in pipe_definitions:
                operator = self.__instance_from_descriptor(operator_descriptor)
                pipeline.append(operator)
        return pipeline
