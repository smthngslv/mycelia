"""Message: ${message}

Revision ID: ${up_revision}
Revises: ${down_revision if down_revision else "-"}
Create Date: ${create_date}
"""

from collections.abc import Sequence
from typing import Final

from alembic import op
${imports if imports else ""}

from mycelia.services.storage.postgres.types import UTCDateTime

__all__: Final[tuple[str, ...]] = ("branch_labels", "depends_on", "down_revision", "downgrade", "revision", "upgrade")

# Revision identifiers, used by Alembic.
revision: Final[str] = ${repr(up_revision)}
down_revision: Final[str | None] = ${repr(down_revision)}
branch_labels: Final[str | Sequence[str] | None] = ${repr(branch_labels)}
depends_on: Final[str | Sequence[str] | None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
