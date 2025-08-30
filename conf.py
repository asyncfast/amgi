import sys
from pathlib import Path

from multiproject.utils import get_project

copyright = "2025, Jack Burridge"
author = "Jack Burridge"
release = "0.11.0"


exclude_patterns = ["_build", ".venv"]


extensions = ["multiproject", "myst_parser", "sphinx_inline_tabs", "sphinx_copybutton"]

myst_enable_extensions = [
    "fieldlist",
]


multiproject_projects = {
    "asyncfast": {
        "path": "packages/asyncfast/docs/",
        "config": {
            "project": "AsyncFast",
        },
        "use_config_file": False,
    },
}

current_project = get_project(multiproject_projects)


if current_project == "asyncfast":
    sys.path.append(
        str((Path(".") / "packages" / "asyncfast" / "docs" / "_ext").resolve())
    )
    print(sys.path)

    extensions += ["async_fast_example"]

html_theme = "furo"
