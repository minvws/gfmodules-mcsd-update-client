import importlib
import pkgutil

"""
This file is used to import all submodules of a module, recursively, including subpackages. This is needed since
we dynamically load the repository classes and we need to import them in order to use them.
"""


def import_submodules(package_name: str) -> None:
    """
    Import all submodules of a module, recursively, including subpackages

    :param package_name:
    :return:
    """
    package = importlib.import_module(package_name)
    for loader, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        full_module_name = f"{package_name}.{module_name}"
        if is_pkg:
            import_submodules(full_module_name)
        else:
            importlib.import_module(full_module_name)


import_submodules('app.db.repository')
