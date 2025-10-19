from datetime import UTC, datetime
from typing import Final, Self, final

from sqlalchemy import Dialect, TypeDecorator
from sqlalchemy.dialects.postgresql import TIMESTAMP

__all__: Final[tuple[str, ...]] = ("UTCDateTime",)


@final
class UTCDateTime(TypeDecorator):
    impl: type[TIMESTAMP] = TIMESTAMP
    cache_ok: bool = True

    def process_bind_param(self: Self, /, value: datetime | None, _: Dialect) -> datetime | None:
        if value is None:
            return None

        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)

        message: Final[str] = "Using datetime without timezone is not allowed."
        raise ValueError(message)

    def process_result_value(self: Self, /, value: datetime | None, _: Dialect) -> datetime | None:
        return value.replace(tzinfo=UTC) if value is not None else None
