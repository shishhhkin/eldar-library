from fastapi import APIRouter

from src.api.schemas.healthcheck import HealthResponse

router = APIRouter()


@router.get('/healthcheck', response_model=HealthResponse)
async def healthcheck() -> HealthResponse:
    return HealthResponse(status='ok')
