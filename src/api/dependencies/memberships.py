from typing import Annotated

from fastapi import Depends

from src.api.dependencies.unit_of_work import UnitOfWorkDep
from src.application.use_cases import GetMembership, IssueMembership


def _issue_membership(uow: UnitOfWorkDep) -> IssueMembership:
    return IssueMembership(uow)


def _get_membership(uow: UnitOfWorkDep) -> GetMembership:
    return GetMembership(uow)


IssueMembershipDep = Annotated[IssueMembership, Depends(_issue_membership)]
GetMembershipDep = Annotated[GetMembership, Depends(_get_membership)]
