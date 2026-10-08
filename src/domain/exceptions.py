from uuid import UUID


class DomainError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class MembershipNotFoundError(DomainError):
    def __init__(self, membership_id: UUID) -> None:
        super().__init__(f'Membership {membership_id} not found')
