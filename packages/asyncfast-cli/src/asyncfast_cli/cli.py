import importlib
import json
import os
import sys
from importlib.metadata import entry_points
from typing import Annotated
from typing import Any

import typer
from amgi_types import AMGIApplication
from typer.main import get_command


def import_from_string(import_str: Any) -> Any:
    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = (
            'Import string "{import_str}" must be in format "<module>:<attribute>".'
        )
        raise Exception(message.format(import_str=import_str))

    try:
        module = importlib.import_module(module_str)
    except ModuleNotFoundError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Could not import module "{module_str}".'
        raise Exception(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise Exception(message.format(attrs_str=attrs_str, module_str=module_str))

    return instance


app = typer.Typer()


@app.command()
def asyncapi(app: str) -> None:
    loaded_app = import_from_string(app)
    print(json.dumps(loaded_app.asyncapi(), indent=2))


@app.callback()
def callback() -> None:
    pass


run_app = typer.Typer()
app.add_typer(run_app, name="run")

for entry_point in entry_points().select(group="amgi_server"):
    try:
        test_app = typer.Typer()
        function = entry_point.load()

        for name, annotation in function.__annotations__.items():
            if annotation is AMGIApplication:
                function.__annotations__[name] = Annotated[
                    AMGIApplication, typer.Argument(parser=import_from_string)
                ]
        test_app.command(entry_point.name)(function)
        get_command(test_app)
        run_app.command(entry_point.name)(function)
    except RuntimeError:
        pass


def main() -> None:
    sys.path.insert(0, os.getcwd())
    app()
