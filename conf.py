import sys
from pathlib import Path

from multiproject.utils import get_project

copyright = "2025, Jack Burridge"
author = "Jack Burridge"
release = "0.18.0"


exclude_patterns = ["_build", ".venv"]


extensions = [
    "multiproject",
    "sphinx_inline_tabs",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}


multiproject_projects = {
    "asyncfast": {
        "path": "packages/asyncfast/docs/",
        "config": {
            "project": "AsyncFast",
        },
        "use_config_file": False,
    },
    "amgi": {
        "path": "docs/",
        "config": {
            "project": "AMGI",
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
