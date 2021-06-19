import builtins
import traceback
from importlib import import_module
from typing import Union


def load_class(full_qualified_classname: str):
    type_parts = full_qualified_classname.split('.')
    classname = type_parts[-1]
    module_path = type_parts[:-1]
    if len(module_path) > 0:
        module = import_module('.'.join(module_path))
    else:
        module = globals()
        if classname not in module:
            module = builtins
    return getattr(module, classname)


load_function = load_class


def create_instance(clazz, arguments: Union[dict, list]):
    if isinstance(arguments, dict):
        return clazz(**arguments)
    elif isinstance(arguments, list):
        try:
            return clazz(*arguments)
        except TypeError:
            traceback.print_exc()
    return clazz()
