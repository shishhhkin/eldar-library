from src.api.schemas.memberships import MembershipRead
from src.domain.membership import Membership


def to_membership_read(membership: Membership) -> MembershipRead:
    return MembershipRead(
        id=membership.id,
        user_id=membership.user_id,
        number=membership.number.value,
        issued_at=membership.issued_at,
    )
