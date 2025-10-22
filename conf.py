import sys
from pathlib import Path

from multiproject.utils import get_project

copyright = "2025, Jack Burridge"
author = "Jack Burridge"
release = "0.15.0"


exclude_patterns = ["_build", ".venv"]


extensions = ["multiproject", "sphinx_inline_tabs", "sphinx_copybutton"]


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

    extensions += ["async_fast_example"]

html_theme = "furo"
