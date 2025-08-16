import json
import textwrap
from typing import Any
from typing import Dict
from typing import Sequence

from asyncfast import AsyncFast
from docutils.nodes import Node
from docutils.parsers.rst import Directive
from sphinx.application import Sphinx


class AsyncFastExample(Directive):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    def run(self) -> Sequence[Node]:
        warning = self.state.document.reporter.warning
        doc_source_name = self.state.document.attributes["source"]

        filename = self.arguments[0]

        _, absolute_path = self.state.document.settings.env.relfn2path(filename)

        try:
            with open(absolute_path) as file:
                async_fast_example = file.read()
                scope: Dict[str, Any] = {}
                try:
                    exec(async_fast_example, scope)
                except Exception as exc:
                    return [
                        warning(
                            f"Error in example: {exc}",
                            line=self.lineno,
                        )
                    ]

                app = scope.get("app")
                if app is None:
                    return [
                        warning(
                            "Could not find 'app' in file",
                            line=self.lineno,
                        )
                    ]
                if not isinstance(app, AsyncFast):
                    return [
                        warning(
                            f"Object 'app' is not an instance of AsyncFast",
                            line=self.lineno,
                        )
                    ]

                try:
                    asyncapi = app.asyncapi()
                except Exception as exc:
                    return [
                        warning(
                            f"Error generating asyncapi: {exc}",
                            line=self.lineno,
                        )
                    ]
        except FileNotFoundError:
            return [
                warning(
                    f"File '{filename}' not found",
                    line=self.lineno,
                )
            ]
        asyncapi_lines = textwrap.indent(
            json.dumps(asyncapi, indent=2), "        "
        ).splitlines()

        lines = [
            ".. tab:: Example",
            "    :new-set:",
            "",
            f"    .. literalinclude:: {filename}",
            "",
            ".. tab:: AsyncAPI",
            "",
            "    .. code:: json",
            "",
            *asyncapi_lines,
        ]

        self.state_machine.insert_input(lines, source=doc_source_name)

        return []


def setup(app: Sphinx) -> None:
    app.add_directive("async-fast-example", AsyncFastExample)
