import logging

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from devrelay.api.errors import ApiErrorCode, api_error_response

logger = logging.getLogger(__name__)


class ResponseAbortedError(RuntimeError):
    """Signal response failure without exposing the underlying exception."""


class UnexpectedErrorMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = False

        async def tracked_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, receive, tracked_send)
        except Exception as error:
            logger.error(
                "Unexpected HTTP failure.",
                extra={"error_type": type(error).__name__},
            )
            if started:
                # The server must abort an incomplete response; never re-raise
                # the original error for ServerErrorMiddleware/Uvicorn to log.
                raise ResponseAbortedError(
                    "HTTP response failed after it started."
                ) from None
            try:
                await api_error_response(
                    500, ApiErrorCode.INTERNAL_ERROR, "Internal error."
                )(scope, receive, tracked_send)
            except Exception as response_error:
                logger.error(
                    "HTTP error response could not be sent.",
                    extra={"error_type": type(response_error).__name__},
                )
                raise ResponseAbortedError(
                    "HTTP error response could not be sent."
                ) from None
