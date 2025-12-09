import subprocess
from argparse import ArgumentParser
from pathlib import Path

import tomli
import tomli_w
from packaging.requirements import Requirement


def get_latest_tag_name() -> str:
    process = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0"], capture_output=True, text=True
    )

    process.check_returncode()
    return process.stdout.strip()


def main(pyproject_paths: list[Path]) -> None:
    latest_tag_name = get_latest_tag_name()
    for pyproject_path in pyproject_paths:
        update = False
        with pyproject_path.open("rb") as pyproject_file:
            pyproject = tomli.load(pyproject_file)
            if "build-system" not in pyproject:
                continue

        project = pyproject["project"]
        version = project["version"]
        if version != latest_tag_name:
            update = True
            project["version"] = latest_tag_name

        dependencies = project.get("dependencies", [])
        dependencies_versioned = {
            Requirement(dependency).name: dependency for dependency in dependencies
        }

        tool_uv_sources = pyproject.get("tool", {}).get("uv", {}).get("sources", {})
        for name in tool_uv_sources:
            if name in dependencies_versioned:
                name_versioned = dependencies_versioned[name]
                name_latest_tag_name = f"{name}=={latest_tag_name}"
                if name_versioned != name_latest_tag_name:
                    update = True
                    dependencies[dependencies.index(name_versioned)] = (
                        name_latest_tag_name
                    )

        if update:
            with pyproject_path.open("wb") as pyproject_file:
                tomli_w.dump(pyproject, pyproject_file, indent=2)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("pyproject_paths", nargs="*", type=Path)
    main(parser.parse_args().pyproject_paths)
