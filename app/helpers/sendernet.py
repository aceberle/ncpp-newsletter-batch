import os

from aiohttp import ClientResponseError

SENDERNET_ROOT_URL = 'https://api.sender.net/v2'
SENDERNET_TOKEN = os.getenv('SENDERNET_TOKEN')
SENDER_HEADERS = {
    'authorization': f'Bearer {SENDERNET_TOKEN}',
    "Accept": "application/json"
}


async def get_subscriber(session, email):
    url = f'{SENDERNET_ROOT_URL}/subscribers/{email}'
    try:
        async with session.get(
                url,
                headers=SENDER_HEADERS) as resp:
            return (await resp.json())['data']
    except ClientResponseError as error:
        if(error.status == 404):
            return None
        else:
            raise error


async def get_paginated(session, url):
    results = []
    while url:
        async with session.get(
                url,
                headers=SENDER_HEADERS) as resp:
            response = (await resp.json())
            results = results + response['data']
            if(response['links']):
                url = response['links']['next']
            else:
                break
    return results


async def get_groups(session):
    url = f'{SENDERNET_ROOT_URL}/groups'
    return await get_paginated(session, url)


async def get_fields(session):
    url = f'{SENDERNET_ROOT_URL}/fields'
    return await get_paginated(session, url)


async def create_group(session, title):
    url = f'{SENDERNET_ROOT_URL}/groups'
    payload = {'title': title}
    async with session.post(
            url,
            headers=SENDER_HEADERS,
            json=payload) as resp:
        response = await resp.json()
        if(response['success']):
            return response['data']['id']
        else:
            raise Exception(response['message'])


async def delete_group(session, group_id):
    url = f'{SENDERNET_ROOT_URL}/groups/{group_id}'
    async with session.delete(
            url,
            headers=SENDER_HEADERS) as resp:
        response = await resp.json()
        if(not response['success']):
            raise Exception(response['message'])
        return response


async def update_subscriber(session, email, payload):
    url = f'{SENDERNET_ROOT_URL}/subscribers/{email}'
    async with session.patch(
            url,
            headers=SENDER_HEADERS,
            json=payload) as resp:
        response = await resp.json()
        if(not response['success']):
            raise Exception(response['message'])
        return response


async def create_subscriber(session, email, payload):
    url = f'{SENDERNET_ROOT_URL}/subscribers'
    payload['email'] = email
    async with session.post(
            url,
            headers=SENDER_HEADERS,
            json=payload) as resp:
        response = await resp.json()
        if(not response['success']):
            raise Exception(response['message'])
        return response
