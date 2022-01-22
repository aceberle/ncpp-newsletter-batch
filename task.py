from typing import Dict
from google.oauth2 import service_account  # type: ignore
import google.auth.transport.requests  # type: ignore
from helpers.clientsession import get_client_session
import logging
import asyncio
import os
from dotenv import load_dotenv
from aiohttp import ClientResponseError 

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/userinfo.email']
SERVICE_ACCOUNT_FILE = 'websiteaccess.json'
DIRECTORY_ROOL_URL = 'https://www.ncpilgrimage.com/api'
SENDERNET_ROOT_URL = 'https://api.sender.net/v2'
SENDERNET_TOKEN = os.getenv('SENDERNET_TOKEN')
SENDER_HEADERS = {
    'authorization': f'Bearer {SENDERNET_TOKEN}',
    "Accept": "application/json"
}
DIRECTORY_HEADERS: Dict[str, str] = {}
PIEDMONT_CONFERENCE = 'Piedmont Conference'
WESTERN_CONFERENCE = 'Western Conference'
EASTERN_CONFERENCE = 'Eastern Conference'
CONFERENCES = {
    PIEDMONT_CONFERENCE,
    WESTERN_CONFERENCE,
    EASTERN_CONFERENCE
}
LOCATION_TO_CONFERENCE = {
    'Camp Weaver': PIEDMONT_CONFERENCE,
    'Camp Hanes': PIEDMONT_CONFERENCE,
    'Camp New Hope': PIEDMONT_CONFERENCE,
    'Laurel Ridge Moravian Conference Center': PIEDMONT_CONFERENCE,
    'Camp Dogwood': PIEDMONT_CONFERENCE,
    'Camp Harrison': WESTERN_CONFERENCE,
    'Camp Dixie': EASTERN_CONFERENCE
}
NEWSLETTER_NEWS = 'Newsletter NCPP News'
NEWSLETTER_PRAYER = 'Newsletter NCPP Prayer and Praise'
NEWSLETTERS = [
    NEWSLETTER_NEWS,
    NEWSLETTER_PRAYER
]


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


def update_payload(payload, fieldname, expected_value, subscriber=None):
    if(subscriber is None or subscriber[fieldname] != expected_value):
        payload[fieldname] = expected_value


def update_groups(payload, group_ids, subscriber=None):
    if(subscriber is None):
        cur_group_ids = set()
    else:
        cur_group_ids = set(map(
            lambda group: group['id'], subscriber['subscriber_tags']))
    new_group_ids = cur_group_ids.union(group_ids)
    if(len(cur_group_ids) < len(new_group_ids)):
        payload['groups'] = list(new_group_ids)


def set_weekend_fields(new_fields, prefix, weekend):
    new_fields[f'{{${prefix}_weekend_date}}'] = weekend['date']
    new_fields[f'{{${prefix}_weekend_number}}'] = weekend['week_id']
    new_fields[f'{{${prefix}_weekend_location}}'] = weekend['location']


def update_fields(payload, pilgrim, field_name_by_title, subscriber=None):
    cur_fields = {}
    if(subscriber is not None):
        for column in subscriber['columns']:
            field_name = field_name_by_title[column['title']]
            cur_fields[field_name] = column['value']
    new_fields = dict(cur_fields)
    roles = pilgrim['roles']
    if(len(roles)):
        first_weekend = roles[0]
        last_weekend = roles[len(roles)-1]
        if(int(first_weekend['role_type_guest'])):
            set_weekend_fields(new_fields, 'guest', first_weekend)
        set_weekend_fields(new_fields, 'last', last_weekend)
    new_fields['{$pilgrim_id}'] = pilgrim['pilgrim_id']
    if(pilgrim['church']):
        new_fields['{$church}'] = pilgrim['church'].replace("'", "")
    elif('{$church}' in cur_fields):
        new_fields['{$church}'] = ''
    weekends_served = list(filter(
        lambda week: not int(week['role_type_guest']), roles))
    new_fields['{$number_of_weekends_served}'] = str(len(weekends_served))
    if(cur_fields != new_fields):
        payload['fields'] = new_fields


async def update_subscriber(session, pilgrim, field_name_by_title):
    email = pilgrim['email'].lower()
    subscriber = await get_subscriber(session, email)
    payload = {}
    update_payload(payload, 'firstname', pilgrim['first_name'], subscriber)
    update_payload(payload, 'lastname', pilgrim['last_name'], subscriber)
    update_groups(payload, pilgrim['group_ids'], subscriber)
    update_fields(payload, pilgrim, field_name_by_title, subscriber)
    try:
        if(subscriber):
            url = f'{SENDERNET_ROOT_URL}/subscribers/{email}'
            #if(len(payload)):
            #    async with session.patch(
            #            url,
            #            headers=SENDER_HEADERS,
            #            json=payload) as resp:
            #        response = await resp.json()
            #        if(not response['success']):
            #            raise Exception(response['message'])
        else:
            url = f'{SENDERNET_ROOT_URL}/subscribers'
            payload['email'] = email
            #async with session.post(
            #        url,
            #        headers=SENDER_HEADERS,
            #        json=payload) as resp:
            #    response = await resp.json()
            #    if(not response['success']):
            #        raise Exception(response['message'])
    except ClientResponseError:
        logger.exception(f'Error occurred while processing email {email}')
    return pilgrim


async def get_pilgrims(session):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims'
    async with session.get(
            url, headers=DIRECTORY_HEADERS) as resp:
        return await resp.json()


async def get_weeks(session):
    url = f'{DIRECTORY_ROOL_URL}/weeks'
    async with session.get(
            url, headers=DIRECTORY_HEADERS) as resp:
        return await resp.json()


async def populate_pilgrim_roles(session, pilgrim):
    pilgrim_id = pilgrim['pilgrim_id']
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}/roles'
    async with session.get(
            url, headers=DIRECTORY_HEADERS) as resp:
        pilgrim['roles'] = await resp.json()
        logger.info(
            'Pilgrim id %s has %s roles',
            pilgrim['pilgrim_id'],
            len(pilgrim['roles']))


async def populate_pilgrim_newsletters(session, pilgrim):
    pilgrim_id = pilgrim['pilgrim_id']
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}/newsletters'
    async with session.get(
            url, headers=DIRECTORY_HEADERS) as resp:
        pilgrim['newsletters'] = await resp.json()
        logger.info(
            'Pilgrim id %s has %s newsletters',
            pilgrim['pilgrim_id'],
            len(pilgrim['newsletters']))


async def populate_additional_pilgrim_data(session, pilgrim):
    await asyncio.gather(
        populate_pilgrim_roles(session, pilgrim),
        populate_pilgrim_newsletters(session, pilgrim)
    )
    return pilgrim


async def get_group_id_by_type_title(session, group_id_by_title, type_titles):
    group_id_by_type_title = {}
    for title in type_titles:
        if(title in group_id_by_title):
            group_id_by_type_title[title] = group_id_by_title[title]
        else:
            group_id_by_type_title[title] = await create_group(session, title)
    return group_id_by_type_title


async def get_field_name_by_title(session):
    fields = await get_fields(session)
    field_name_by_title = {}
    for field in fields:
        field_name_by_title[field['title']] = field['field_name']
    return field_name_by_title


async def main():
    credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    logger.info('Fetched token: ' + credentials.token)
    DIRECTORY_HEADERS['authorization'] = f'Bearer {credentials.token}'
    async with get_client_session() as session:
        # fetch groups
        groups = await get_groups(session)
        group_id_by_title = {}
        for group in groups:
            group_id_by_title[group['title']] = group['id']
        group_id_by_conference = await get_group_id_by_type_title(
            session, group_id_by_title, CONFERENCES)
        group_id_by_newsletter = await get_group_id_by_type_title(
            session, group_id_by_title, NEWSLETTERS)
        logger.info(f'Found {len(group_id_by_conference)} conference groups!')
        # fetch fields
        field_name_by_title = await get_field_name_by_title(session)
        logger.info(f'Found {len(field_name_by_title)} fields!')
        pilgrims = await get_pilgrims(session)
        logger.info(f'Found {len(pilgrims)} pilgrims!')
        pilgrims = list(filter(lambda pilgrim: pilgrim['email'], pilgrims))
        pilgrims = pilgrims[0:100]
        update_pilgrim_tasks = []
        for pilgrim in pilgrims:
            update_pilgrim_tasks.append(
                asyncio.create_task(
                    populate_additional_pilgrim_data(session, pilgrim)))
        update_subscriber_tasks = []
        for task in asyncio.as_completed(update_pilgrim_tasks):
            pilgrim = await task
            pilgrim['group_ids'] = set()
            for role in pilgrim['roles']:
                location = role['location']
                if(location in LOCATION_TO_CONFERENCE):
                    conference = LOCATION_TO_CONFERENCE[location]
                    group_id = group_id_by_conference[conference]
                    pilgrim['group_ids'].add(group_id)
            logger.info('Found %s groups', len(pilgrim['group_ids']))
            update_subscriber_tasks.append(
                asyncio.create_task(
                    update_subscriber(session, pilgrim, field_name_by_title)))
        for task in asyncio.as_completed(update_subscriber_tasks):
            pilgrim = await task
            logger.info(
                'Finished updating subscriber information for %s',
                pilgrim['email'])


asyncio.run(main())
