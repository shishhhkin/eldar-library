import logging
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.middleware import REQUEST_ID_HEADER
from src.api.schemas.errors import ErrorResponse
from src.domain.exceptions import DomainError, MembershipAlreadyExistsError, MembershipNotFoundError

logger = logging.getLogger(__name__)

DOMAIN_ERRORS: dict[type[DomainError], tuple[HTTPStatus, str]] = {
    MembershipNotFoundError: (HTTPStatus.NOT_FOUND, 'not_found'),
    MembershipAlreadyExistsError: (HTTPStatus.CONFLICT, 'already_exists'),
}


def error_response(status: HTTPStatus, code: str, detail: str, request: Request) -> JSONResponse:
    rid = getattr(request.state, 'request_id', None)
    headers = {REQUEST_ID_HEADER: rid} if rid else None
    body = ErrorResponse(code=code, detail=detail, request_id=rid)
    return JSONResponse(status_code=status.value, content=body.model_dump(), headers=headers)


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception('unhandled exception: %s', type(exc).__name__)
    return error_response(
        HTTPStatus.INTERNAL_SERVER_ERROR, 'internal_error', 'Internal server error', request
    )


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    mapped = DOMAIN_ERRORS.get(type(exc))
    if mapped is None:
        return await unhandled_error_handler(request, exc)
    status, code = mapped
    return error_response(status, code, exc.message, request)


def register_exception_handlers(app: FastAPI) -> None:
    app.exception_handler(DomainError)(domain_error_handler)
    app.exception_handler(Exception)(unhandled_error_handler)
