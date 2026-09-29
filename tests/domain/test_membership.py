from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from src.domain.membership import CardNumber, Membership


def test_card_number_from_sequence_pads_to_eight_digits() -> None:
    assert CardNumber.from_sequence(42).value == 'LIB-00000042'


def test_card_number_from_sequence_accepts_max_value() -> None:
    assert CardNumber.from_sequence(99_999_999).value == 'LIB-99999999'


def test_card_number_from_sequence_rejects_overflow() -> None:
    with pytest.raises(ValueError, match='Invalid card number'):
        CardNumber.from_sequence(100_000_000)


def test_card_number_accepts_valid_value() -> None:
    assert CardNumber('LIB-00000001').value == 'LIB-00000001'


@pytest.mark.parametrize(
    'value',
    [
        '',
        'LIB-',
        'LIB-1234567',
        'LIB-123456789',
        'lib-00000042',
        'XYZ-00000042',
        'LIB-0000004a',
        ' LIB-00000042',
        'LIB-00000042 ',
        'LIB-٠٠٠٠٠٠٤٢',
    ],
)
def test_card_number_rejects_invalid_value(value: str) -> None:
    with pytest.raises(ValueError, match='Invalid card number'):
        CardNumber(value)


def test_card_number_compares_by_value() -> None:
    assert CardNumber('LIB-00000007') == CardNumber.from_sequence(7)


def test_card_number_is_immutable() -> None:
    number = CardNumber('LIB-00000001')

    with pytest.raises(FrozenInstanceError):
        number.value = 'LIB-00000002'  # type: ignore[misc]


def test_issue_sets_fields() -> None:
    user_id = uuid4()
    number = CardNumber.from_sequence(1)
    now = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)

    membership = Membership.issue(user_id, number, now)

    assert membership.user_id == user_id
    assert membership.number == number
    assert membership.issued_at == now
    assert membership.id.version == 7


def test_issue_generates_distinct_ids() -> None:
    now = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
    number = CardNumber.from_sequence(1)

    first = Membership.issue(uuid4(), number, now)
    second = Membership.issue(uuid4(), number, now)

    assert first.id != second.id
