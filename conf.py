copyright = "2025, Jack Burridge"
author = "Jack Burridge"
release = "0.9.0"


exclude_patterns = ["_build", ".venv"]


extensions = ["multiproject", "myst_parser"]

multiproject_projects = {
    "asyncfast": {
        "path": "packages/asyncfast/docs/",
        "config": {
            "project": "AsyncFast",
        },
        "use_config_file": False,
    },
}


html_theme = "sphinx_rtd_theme"
