from typing import Protocol

from devrelay.application.contracts.base_sources import (
    BaseSourceRequest,
    BaseSourceSnapshot,
)


class BaseSourceUnavailable(Exception):
    """A bounded source read is unavailable or uncertain."""


class BaseSourceAccessDenied(BaseSourceUnavailable):
    """Source is outside configured repository read authority."""


class InvalidBaseSourceData(BaseSourceUnavailable):
    """Provider data failed bounded source validation."""


class BaseSourcePort(Protocol):
    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot: ...
