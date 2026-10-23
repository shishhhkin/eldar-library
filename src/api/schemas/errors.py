from typing import Any

from pydantic import BaseModel, ConfigDict

_REQUEST_ID_EXAMPLE = '8727204f-be85-4c38-acf4-455ed8188dc0'


class ErrorResponse(BaseModel):
    code: str
    detail: str
    request_id: str | None = None


class NotFoundResponse(ErrorResponse):
    model_config = ConfigDict(
        json_schema_extra={
            'examples': [
                ErrorResponse(
                    code='not_found',
                    detail='Membership 22235be6-92c8-4eee-8a26-b6b05cc323ab not found',
                    request_id=_REQUEST_ID_EXAMPLE,
                ).model_dump(mode='json')
            ]
        }
    )


READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    404: {'model': NotFoundResponse, 'description': 'Membership not found'},
}
