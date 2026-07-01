import configparser
from argparse import ArgumentParser
from pathlib import Path

import yaml


def get_tox_labels(path: Path) -> set[str]:
    config = configparser.ConfigParser(interpolation=None)
    with path.open() as file:
        config.read_file(file)

    labels: set[str] = set()
    for section in config.sections():
        if config.has_option(section, "labels"):
            value = config.get(section, "labels")
            labels.update(value.replace(",", " ").split())
    return labels


def get_test_matrix_labels(path: Path) -> set[str]:
    with path.open() as file:
        workflow = yaml.safe_load(file)

    labels = workflow["jobs"]["tests"]["strategy"]["matrix"]["label"]
    if not isinstance(labels, list) or not all(
        isinstance(label, str) for label in labels
    ):
        raise ValueError("jobs.tests.strategy.matrix.label must be a list of strings")
    return set(labels)


def main(tox_path: Path, workflow_path: Path, excludes: set[str]) -> int:
    missing = (
        get_tox_labels(tox_path) - excludes - get_test_matrix_labels(workflow_path)
    )
    if not missing:
        return 0

    print(f"{workflow_path} is missing tox labels:")
    for label in sorted(missing):
        print(f"  - {label}")
    return 1


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("tox", type=Path)
    parser.add_argument("workflow", type=Path)
    parser.add_argument("--exclude", action="append")
    arguments = parser.parse_args()
    raise SystemExit(
        main(arguments.tox, arguments.workflow, set(arguments.exclude or ()))
    )
