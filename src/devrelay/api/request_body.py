import asyncio

from fastapi import Request
from pydantic import BaseModel, ValidationError
from starlette.requests import ClientDisconnect

from devrelay.api.errors import (
    RequestBodyInterrupted,
    RequestBodyTimeout,
    RequestBodyTooLarge,
    UnsupportedRequestMedia,
)

MAX_BODY_BYTES = 1024


async def read_json_request[Payload: BaseModel](
    request: Request,
    timeout: float,
    model: type[Payload],
    invalid_request: type[Exception],
) -> Payload:
    if (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
        or request.headers.get("content-encoding", "identity").lower() != "identity"
    ):
        raise UnsupportedRequestMedia
    body = bytearray()
    try:
        async with asyncio.timeout(timeout):
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    raise RequestBodyTooLarge
                body.extend(chunk)
    except TimeoutError:
        raise RequestBodyTimeout from None
    except ClientDisconnect:
        raise RequestBodyInterrupted from None
    try:
        return model.model_validate_json(body)
    except ValidationError:
        raise invalid_request from None


def json_body_schema(model: type[BaseModel]) -> dict[str, object]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }
