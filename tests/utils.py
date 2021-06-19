import os


def absolute_path_to(*args):
    return os.path.join(os.path.dirname(__file__), *args)
