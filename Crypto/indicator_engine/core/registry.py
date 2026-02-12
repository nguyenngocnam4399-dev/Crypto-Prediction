# core/registry.py

REGISTRY = {}


def register(name):

    def wrap(func):
        REGISTRY[name] = func
        return func

    return wrap


def get_all():
    return REGISTRY
