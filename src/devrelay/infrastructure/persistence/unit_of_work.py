from types import TracebackType
from typing import Self

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.ports.task_persistence import (
    TaskPersistenceError,
    TaskRepository,
)
from devrelay.infrastructure.persistence.task_repository import SqlAlchemyTaskRepository


class SqlAlchemyTaskUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._tasks: TaskRepository | None = None

    @property
    def tasks(self) -> TaskRepository:
        if self._tasks is None:
            raise RuntimeError("Task unit of work is not active.")
        return self._tasks

    async def __aenter__(self) -> Self:
        if self._session is not None:
            raise RuntimeError("Task unit of work is already active.")
        self._session = self._sessions()
        self._tasks = SqlAlchemyTaskRepository(self._session)
        try:
            await self._session.begin()
        except SQLAlchemyError:
            await self._session.close()
            self._session = None
            self._tasks = None
            raise TaskPersistenceError("Task transaction could not start.") from None
        return self

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Task unit of work is not active.")
        try:
            await self._session.commit()
        except SQLAlchemyError:
            raise TaskPersistenceError("Task transaction could not commit.") from None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._session is None:
            return
        session = self._session
        try:
            try:
                await session.rollback()
            finally:
                await session.close()
        except SQLAlchemyError:
            raise TaskPersistenceError("Task transaction cleanup failed.") from None
        finally:
            self._session = None
            self._tasks = None
