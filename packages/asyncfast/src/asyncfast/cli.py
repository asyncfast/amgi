try:
    from asyncfast_cli.cli import main
except ImportError:

    def main() -> None:
        raise RuntimeError(
            'To use the asyncfast command, please install "asyncfast[standard]":\n\n\tpip install "asyncfast[standard]"\n'
        )


__all__ = ["main"]
