from typing import Annotated

from fastapi import Depends

from src.api.dependencies.db import SessionFactoryDep
from src.application.unit_of_work import UnitOfWork
from src.infrastructure.db.unit_of_work import SqlAlchemyUnitOfWork


def get_unit_of_work(session_factory: SessionFactoryDep) -> UnitOfWork:
    return SqlAlchemyUnitOfWork(session_factory)


UnitOfWorkDep = Annotated[UnitOfWork, Depends(get_unit_of_work)]
