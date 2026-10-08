from uuid import UUID

from fastapi import APIRouter, Response, status

from src.api.dependencies.memberships import (
    FindMembershipByUserDep,
    GetMembershipDep,
    IssueMembershipDep,
)
from src.api.mappers import to_membership_read
from src.api.schemas.errors import READ_RESPONSES
from src.api.schemas.memberships import MembershipCreate, MembershipRead

router = APIRouter(prefix='/memberships', tags=['memberships'])


@router.post(
    '',
    response_model=MembershipRead,
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {
            'model': MembershipRead,
            'description': 'User already has a membership, it is returned as is',
        },
    },
)
async def issue_membership(
    payload: MembershipCreate, use_case: IssueMembershipDep, response: Response
) -> MembershipRead:
    membership, created = await use_case(payload.user_id)
    if not created:
        response.status_code = status.HTTP_200_OK
    return to_membership_read(membership)


@router.get('/{membership_id}', response_model=MembershipRead, responses=READ_RESPONSES)
async def read_membership(membership_id: UUID, use_case: GetMembershipDep) -> MembershipRead:
    return to_membership_read(await use_case(membership_id))


@router.get('', response_model=list[MembershipRead])
async def list_memberships(
    user_id: UUID, use_case: FindMembershipByUserDep
) -> list[MembershipRead]:
    membership = await use_case(user_id)
    return [] if membership is None else [to_membership_read(membership)]
