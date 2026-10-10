from fastapi import APIRouter

from devrelay.api.schemas.health import LivenessResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", response_model=LivenessResponse)
async def get_liveness() -> LivenessResponse:
    return LivenessResponse()
