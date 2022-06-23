from asyncio import Task
from typing import Dict
import sys
from aiohttp_retry import asyncio
from google.oauth2 import service_account  # type: ignore
import google.auth.transport.requests  # type: ignore
import logging
import os
import base64
import json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/userinfo.email']
SERVICE_ACCOUNT_FILE = 'websiteaccess.json'
IS_PROD = 'prod-dir' in sys.argv[1:]
DIRECTORY_ROOL_URL = (
    'https://www.ncpilgrimage.com/api' if IS_PROD
    else 'http://pilgrimage.localtest.me/api')
logger.info(
    f'IS_PROD={IS_PROD}, using Directory Root URL: {DIRECTORY_ROOL_URL}')
DIRECTORY_COOKIES: Dict[str, str] = {}
fetch_token_task = False
CI_COOKIE = 'ci_session'
GET = 'GET'
PUT = 'PUT'
POST = 'POST'
DELETE = 'DELETE'


async def _fetch_token(session):
    encoded = os.getenv('GOOGLE_ACCOUNT_CREDS')
    decoded = base64.b64decode(encoded)
    account_info = json.loads(decoded.decode('ascii'))
    credentials = service_account.Credentials.from_service_account_info(
            account_info, scopes=SCOPES)
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    # logger.info('Fetched google token: ' + credentials.token)
    payload = {
        'google_token': credentials.token
    }
    url = f'{DIRECTORY_ROOL_URL}/auth/token'
    response = await session.post(url, json=payload)
    data = await response.json()
    DIRECTORY_COOKIES[CI_COOKIE] = data['token']


def _check_token(session):
    global fetch_token_task
    if(fetch_token_task):
        return fetch_token_task
    fetch_token_task = asyncio.create_task(_fetch_token(session))
    return fetch_token_task


CONTENT_TYPE = 'CONTENT-TYPE'


async def do_call(session, method, url, **kwargs):
    await _check_token(session)
    async with session.request(
            method,
            url,
            cookies=DIRECTORY_COOKIES,
            raise_for_status=False,
            **kwargs) as resp:
        if(resp.status < 400):
            if(CONTENT_TYPE in resp.headers):
                content_type = resp.headers[CONTENT_TYPE]
                if(content_type.startswith('application/json')):
                    return await resp.json()
                else:
                    raise Exception(f'Unknown content type: {content_type}')
        else:
            body = await resp.text()
            logger.error(
                f'Call to {method} {url} failed! '
                f'Status={resp.status} '
                f'Body={body}'
            )
            resp.raise_for_status()


async def get_auth(session):
    url = f'{DIRECTORY_ROOL_URL}/auth'
    return await do_call(session, GET, url)


async def get_weeks(session):
    url = f'{DIRECTORY_ROOL_URL}/weeks'
    return await do_call(session, GET, url)


async def get_conferences(session):
    url = f'{DIRECTORY_ROOL_URL}/conferences'
    return await do_call(session, GET, url)


async def get_pilgrim_ids_to_sync(session):
    url = f'{DIRECTORY_ROOL_URL}/newsletter-sub-sync'
    return await do_call(session, GET, url)


async def clear_pilgrim_ids_to_sync(session, pilgrim_ids):
    url = f'{DIRECTORY_ROOL_URL}/newsletter-sub-sync'
    return await do_call(session, DELETE, url, json={
        'pilgrim_ids': pilgrim_ids
    })


async def get_pilgrim(session, pilgrim_id):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}'
    return await do_call(session, GET, url)


async def get_pilgrims(session):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims'
    return await do_call(session, GET, url)


async def get_newsletters(session):
    url = f'{DIRECTORY_ROOL_URL}/newsletters'
    return await do_call(session, GET, url)


async def get_pilgrim_roles(session, pilgrim_id):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}/roles'
    return await do_call(session, GET, url)


async def get_pilgrim_newsletters(session, pilgrim_id):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}/newsletters'
    return await do_call(session, GET, url)


async def add_pilgrim_newsletters(
        session, pilgrim_id, newsletter_ids):
    url = f'{DIRECTORY_ROOL_URL}/pilgrims/{pilgrim_id}/newsletters'
    return await do_call(session, POST, url, json={
        'newsletter_ids': newsletter_ids
    })


async def update_week(session, week_id, week):
    url = f'{DIRECTORY_ROOL_URL}/weeks/{week_id}'
    return await do_call(session, PUT, url, json=week)
