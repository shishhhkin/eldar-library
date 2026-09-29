from uuid import UUID

from fastapi import APIRouter, status

from src.api.dependencies.memberships import GetMembershipDep, IssueMembershipDep
from src.api.mappers import to_membership_read
from src.api.schemas.errors import CREATE_RESPONSES, READ_RESPONSES
from src.api.schemas.memberships import MembershipCreate, MembershipRead

router = APIRouter(prefix='/memberships', tags=['memberships'])


@router.post(
    '',
    response_model=MembershipRead,
    status_code=status.HTTP_201_CREATED,
    responses=CREATE_RESPONSES,
)
async def issue_membership(
    payload: MembershipCreate, use_case: IssueMembershipDep
) -> MembershipRead:
    return to_membership_read(await use_case(payload.user_id))


@router.get('/{membership_id}', response_model=MembershipRead, responses=READ_RESPONSES)
async def read_membership(membership_id: UUID, use_case: GetMembershipDep) -> MembershipRead:
    return to_membership_read(await use_case(membership_id))
