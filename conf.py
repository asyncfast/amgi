import sys
from pathlib import Path

from multiproject.utils import get_project

copyright = "2025, Jack Burridge"
author = "Jack Burridge"
release = "0.37.0"


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

html_theme_options = {
    "source_repository": "https://github.com/asyncfast/amgi",
    "source_branch": "main",
}


if current_project == "asyncfast":
    html_theme_options["source_directory"] = "packages/asyncfast/docs"
    sys.path.append(
        str((Path(".") / "packages" / "asyncfast" / "docs" / "_ext").resolve())
    )

    extensions += ["async_fast_example"]
if current_project == "amgi":
    html_theme_options["source_directory"] = "docs"
    sys.path.append(str((Path(".") / "docs" / "_ext").resolve()))

    extensions += ["typeddict"]


html_theme = "furo"
