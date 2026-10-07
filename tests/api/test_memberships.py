import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient

URL = '/v1/memberships'


async def test_issue_membership(client: AsyncClient) -> None:
    user_id = uuid4()

    response = await client.post(URL, json={'user_id': str(user_id)})

    assert response.status_code == 201
    body = response.json()
    assert set(body) == {'id', 'user_id', 'number', 'issued_at', 'version'}
    assert UUID(body['id']).version == 7
    assert body['user_id'] == str(user_id)
    assert body['number'] == 'LIB-00000001'
    assert body['version'] == 1
    issued_at = datetime.fromisoformat(body['issued_at'])
    assert abs(datetime.now(UTC) - issued_at) < timedelta(seconds=10)


async def test_read_membership(client: AsyncClient) -> None:
    created = (await client.post(URL, json={'user_id': str(uuid4())})).json()

    response = await client.get(f'{URL}/{created["id"]}')

    assert response.status_code == 200
    assert response.json() == created


async def test_read_membership_not_found(client: AsyncClient) -> None:
    missing_id = uuid4()

    response = await client.get(f'{URL}/{missing_id}')

    assert response.status_code == 404
    body = response.json()
    assert body['code'] == 'not_found'
    assert body['detail'] == f'Membership {missing_id} not found'
    assert body['request_id'] == response.headers['X-Request-ID']


async def test_read_membership_invalid_id(client: AsyncClient) -> None:
    response = await client.get(f'{URL}/not-a-uuid')

    assert response.status_code == 422


async def test_list_memberships_by_user(client: AsyncClient) -> None:
    created = (await client.post(URL, json={'user_id': str(uuid4())})).json()
    await client.post(URL, json={'user_id': str(uuid4())})

    response = await client.get(URL, params={'user_id': created['user_id']})

    assert response.status_code == 200
    assert response.json() == [created]


async def test_list_memberships_by_user_without_membership(client: AsyncClient) -> None:
    response = await client.get(URL, params={'user_id': str(uuid4())})

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.parametrize('params', [{}, {'user_id': 'not-a-uuid'}])
async def test_list_memberships_invalid_user_id(client: AsyncClient, params: dict) -> None:
    response = await client.get(URL, params=params)

    assert response.status_code == 422


@pytest.mark.parametrize(
    'payload',
    [{}, {'user_id': None}, {'user_id': 'not-a-uuid'}, {'user_id': 42}],
)
async def test_issue_membership_invalid_body(client: AsyncClient, payload: dict) -> None:
    response = await client.post(URL, json=payload)

    assert response.status_code == 422


async def test_issue_membership_twice_returns_existing(client: AsyncClient) -> None:
    user_id = uuid4()
    first = (await client.post(URL, json={'user_id': str(user_id)})).json()

    response = await client.post(URL, json={'user_id': str(user_id)})

    assert response.status_code == 200
    assert response.json() == first
    assert (await client.get(URL, params={'user_id': str(user_id)})).json() == [first]


async def test_concurrent_issue_for_same_user_creates_one(client: AsyncClient) -> None:
    payload = {'user_id': str(uuid4())}

    responses = await asyncio.gather(*(client.post(URL, json=payload) for _ in range(5)))

    assert sorted(r.status_code for r in responses) == [200, 200, 200, 200, 201]
    assert len({r.json()['id'] for r in responses}) == 1


async def test_numbers_are_unique_and_increasing(client: AsyncClient) -> None:
    first = (await client.post(URL, json={'user_id': str(uuid4())})).json()
    second = (await client.post(URL, json={'user_id': str(uuid4())})).json()

    assert first['number'] == 'LIB-00000001'
    assert second['number'] == 'LIB-00000002'


async def test_request_id_echoed_from_header(client: AsyncClient) -> None:
    response = await client.get(f'{URL}/{uuid4()}', headers={'X-Request-ID': 'trace-abc'})

    assert response.headers['X-Request-ID'] == 'trace-abc'
    assert response.json()['request_id'] == 'trace-abc'


async def test_unhandled_error_shape(unreachable_client: AsyncClient) -> None:
    response = await unreachable_client.get(f'{URL}/{uuid4()}')

    assert response.status_code == 500
    body = response.json()
    assert body['code'] == 'internal_error'
    assert body['detail'] == 'Internal server error'
    assert body['request_id'] == response.headers['X-Request-ID']
