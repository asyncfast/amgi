# Contributing to AMGI

## Development

The project is structured as a monorepo, with all deployed packages within the `packages/` directory.

uv is used as the package and project manager. Instructions for installation are available
[here][uv installation].

When first installing the project run:

```commandline
uv sync --all-packages
```

This will install all packages, and development dependencies.

### Git

The git branching model used is [Trunk-based development]. A linear code history must be preserved with all commits in
the [Conventional Commits] style.

[Commitizen] is included with the development dependencies, and can be run with writing Conventional Style commits with the `cz commit` command.

### Tests

Tests are runnable via [tox], this will run against multiple Python versions. This is also installed along with the
development dependencies.

[pre-commit] is also ran along with all the tests.

All tests should be written with [pytest], and any integration test should use [testcontainers-python].

### Typing

All packages must be strictly typed where possible.

### Versioning

While in the early stages of development, and as the APIs are not stable, synchronized versioning is used between the
packages. Package dependencies between packages in the monorepo should also be pinned.

[commitizen]: https://commitizen-tools.github.io/commitizen/
[conventional commits]: https://www.conventionalcommits.org/en/v1.0.0/
[pre-commit]: https://pre-commit.com/
[pytest]: https://docs.pytest.org/en/stable/
[testcontainers-python]: https://testcontainers-python.readthedocs.io/en/latest/
[tox]: https://github.com/tox-dev/tox
[trunk-based development]: https://www.atlassian.com/continuous-delivery/continuous-integration/trunk-based-development
[uv installation]: https://docs.astral.sh/uv/getting-started/installation/
