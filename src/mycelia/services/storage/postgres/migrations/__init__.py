import sys
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Final

import alembic.config

__all__: Final[tuple[str, ...]] = ("run",)


def run() -> None:
    with NamedTemporaryFile(mode="wt", suffix=".ini") as file:
        file.write(f"[alembic]\nscript_location = {Path(__file__).parent}\n")
        file.flush()
        alembic.config.main(argv=("--config", file.name, *sys.argv[1:]))
