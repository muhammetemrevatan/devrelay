from typing import Protocol

from devrelay.application.contracts.base_sources import BaseSourceRequest


class GitCommitSource(Protocol):
    async def read(self, request: BaseSourceRequest, *, root_tree_sha: str) -> bytes:
        """Read bounded original commit bytes for the accepted fixed GitHub source."""
        ...
