import json
import os
import traceback
from abc import abstractmethod, ABC
from collections import Iterator, Iterable
from logging import Logger
from typing import Union, Any, Callable

from rx import Observable, from_, catch
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
        def on_error(error):
            traceback.print_exc()
        observable: Observable = from_(self.__iterator).pipe(*self.__ducts)
        if isinstance(self.__observer, InitializingObserver):
            self.__observer.initialize(self)
        observable.subscribe(self.__observer, on_error=on_error)
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
    def __init__(self, working_dir: str = os.path.abspath(os.getcwd())):
        self.__working_dir = working_dir
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
        scope = {}
        scope.update(self.__variables)
        scope.update(self.__named_classes)
        return eval(value[1:], scope)

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

    def __bootstrapp_variables(self, definitions: list[dict]):
        if isinstance(definitions, (Iterable, Iterator, list, tuple)):
            for descriptor in definitions:
                if 'name' not in descriptor:
                    raise ResourceWarning('A variable descriptor needs a name definition.')
                name = descriptor['name']
                self.__variables[name] = self.instance_from_descriptor(descriptor)

    def __bootstrap_include(self, path: Union[list, str]):
        if isinstance(path, str):
            path = [path]
        for p in path:
            if os.path.isabs(p):
                include_path = p
            else:
                include_path = os.path.join(self.__working_dir, p)
            with open(include_path, 'r') as fd:
                self.bootstrap(json.load(fd))

    def instance_from_descriptor(self, descriptor: dict) -> Any:
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

    def bootstrap(self, definitions: dict):
        if 'include' in definitions:
            self.__bootstrap_include(definitions['include'])
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
        iterator = self.instance_from_descriptor(definitions['iterator'])
        observer = None
        if 'observer' in definitions:
            observer = self.instance_from_descriptor(definitions['observer'])
        pipeline = Pipeline(iterator, observer)
        if 'pipe' in definitions:
            pipe_definitions = definitions['pipe']
            for operator_descriptor in pipe_definitions:
                operator = self.instance_from_descriptor(operator_descriptor)
                if isinstance(operator, OperatorBuilder):
                    operator.assembler = self
                    operator = operator.build()
                pipeline.append(operator)
        return pipeline


class OperatorBuilder(ABC):
    def __init__(self):
        self.__assembler = None

    @property
    def assembler(self):
        return self.__assembler

    @assembler.setter
    def assembler(self, assembler: Assembler):
        self.__assembler = assembler

    @abstractmethod
    def build(self) -> Callable[[Observable], Observable]:
        pass
