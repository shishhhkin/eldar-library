from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

_USER_ID_EXAMPLE = '0199a1b2-7c3d-7e4f-8a5b-6c7d8e9f0a1b'


class MembershipCreate(BaseModel):
    user_id: UUID

    model_config = ConfigDict(
        json_schema_extra={'examples': [{'user_id': _USER_ID_EXAMPLE}]},
    )


class MembershipRead(BaseModel):
    id: UUID
    user_id: UUID
    number: str
    issued_at: datetime

    model_config = ConfigDict(
        json_schema_extra={
            'examples': [
                {
                    'id': '0199a1b3-1f2e-7d4c-9b8a-7f6e5d4c3b2a',
                    'user_id': _USER_ID_EXAMPLE,
                    'number': 'LIB-00000042',
                    'issued_at': '2026-09-28T12:00:00Z',
                }
            ]
        },
    )
