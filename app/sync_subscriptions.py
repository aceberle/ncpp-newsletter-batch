from helpers.clientsession import get_client_session
import logging
import asyncio
import re
import json
from dotenv import load_dotenv
from aiohttp import ClientResponseError
import helpers.sendernet as sendernet
import helpers.directory as directory
from helpers.chunker import get_chunks

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Counter(object):
    value = 0

    def increment(self):
        self.value += 1


class Stats(object):
    updated = Counter()
    not_updated = Counter()
    created = Counter()
    errors = Counter()


def update_payload(payload, fieldname, expected_value, subscriber=None):
    expected_value = expected_value.strip()
    if(subscriber is None or subscriber[fieldname] != expected_value):
        payload[fieldname] = expected_value


def update_groups(payload, group_ids, subscriber=None):
    if(subscriber is None):
        cur_group_ids = set()
    else:
        cur_group_ids = set(map(
            lambda group: group['id'], subscriber['subscriber_tags']))
    differences = cur_group_ids.symmetric_difference(group_ids)
    if(len(differences)):
        payload['groups'] = list(group_ids)


def set_weekend_fields(new_fields, prefix, weekend):
    new_fields[f'{{${prefix}_weekend_number}}'] = weekend['week_id']
    if(weekend['date']):
        new_fields[f'{{${prefix}_weekend_date}}'] = weekend['date']
    if(weekend['location']):
        new_fields[f'{{${prefix}_weekend_location}}'] = weekend['location']


ALLOWED_TEXT_FIELD_CHARS = r'[^a-zA-Z0-9\s]'


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
        new_fields['{$church}'] = re.sub(
            ALLOWED_TEXT_FIELD_CHARS, '', pilgrim['church']).strip()
    elif('{$church}' in cur_fields):
        new_fields['{$church}'] = ''
    weekends_served = list(filter(
        lambda week: not int(week['role_type_guest']), roles))
    new_fields['{$number_of_weekends_served}'] = str(len(weekends_served))
    if(cur_fields != new_fields):
        payload['fields'] = new_fields


async def update_subscriber(
        stats, session, pilgrim, field_name_by_title):
    email = pilgrim['email']
    subscriber = await sendernet.get_subscriber(session, email)
    payload = {}
    update_payload(payload, 'firstname', pilgrim['first_name'], subscriber)
    update_payload(payload, 'lastname', pilgrim['last_name'], subscriber)
    update_groups(payload, pilgrim['group_ids'], subscriber)
    update_fields(payload, pilgrim, field_name_by_title, subscriber)
    result: str
    try:
        if(subscriber):
            if(len(payload)):
                await sendernet.update_subscriber(session, email, payload)
                result = 'updated with ' + json.dumps(payload)
                stats.updated.increment()
            else:
                result = 'not updated'
                stats.not_updated.increment()
        else:
            await sendernet.create_subscriber(session, email, payload)
            result = 'created with ' + json.dumps(payload)
            stats.created.increment()
    except ClientResponseError as error:
        logger.exception(f'Error occurred while processing email "{email}"')
        stats.errors.increment()
        result = 'error: ' + repr(error)
    return pilgrim, result


async def populate_pilgrim_roles(session, pilgrim):
    pilgrim_id = pilgrim['pilgrim_id']
    pilgrim['roles'] = await directory.get_pilgrim_roles(session, pilgrim_id)
    logger.info(
        'Pilgrim id %s has %s roles',
        pilgrim['pilgrim_id'],
        len(pilgrim['roles']))


async def populate_pilgrim_newsletters(session, pilgrim):
    pilgrim_id = pilgrim['pilgrim_id']
    pilgrim['newsletters'] = await directory.get_pilgrim_newsletters(
        session, pilgrim_id)
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


async def get_field_name_by_title(session):
    fields = await sendernet.get_fields(session)
    field_name_by_title = {}
    for field in fields:
        field_name_by_title[field['title']] = field['field_name']
    return field_name_by_title


async def create_group_and_add_id(
        session, newsletter_group_ids_by_title, newsletter):
    group_id = await sendernet.create_group(session, newsletter)
    newsletter_group_ids_by_title[newsletter] = group_id


async def get_newsletter_group_ids_by_title(session, newsletters, groups):
    newsletter_group_ids_by_title = {}
    tasks = []
    newsletters = set(
        map(lambda newsletter: newsletter['newsletter_label'], newsletters))
    groups_by_title = {}
    for group in groups:
        groups_by_title[group['title']] = group
    for newsletter in newsletters:
        if(newsletter in groups_by_title):
            newsletter_group_ids_by_title[newsletter] = \
                groups_by_title[newsletter]['id']
            del groups_by_title[newsletter]
        else:
            tasks.append(
                asyncio.create_task(
                    create_group_and_add_id(
                        session, newsletter_group_ids_by_title, newsletter)
                ))
    return newsletter_group_ids_by_title


async def main():
    async with get_client_session() as session:
        newsletters, groups = await asyncio.gather(
            directory.get_newsletters(session),
            sendernet.get_groups(session)
        )
        newsletter_group_ids_by_title = \
            await get_newsletter_group_ids_by_title(
                session, newsletters, groups)
        logger.info(
            f'Found {len(newsletter_group_ids_by_title)} newsletter groups!')
        # fetch fields
        field_name_by_title = await get_field_name_by_title(session)
        logger.info(f'Found {len(field_name_by_title)} fields!')
        pilgrims = await directory.get_pilgrims(session)
        logger.info(f'Found {len(pilgrims)} pilgrims!')
        pilgrims = list(
            filter(
                lambda pilgrim: pilgrim['email'] and pilgrim['email'].strip(),
                pilgrims))
        logger.info(f'Found {len(pilgrims)} pilgrims with email addresses!')
        pilgrims_by_email = {}
        for pilgrim in pilgrims:
            email = pilgrim['email'].lower().strip()
            pilgrim['email'] = email
            pilgrims_by_email[email] = \
                [] if email not in pilgrims_by_email \
                else pilgrims_by_email[email]
            pilgrims_by_email[email].append(pilgrim)
        logger.info(
            f'Found {len(pilgrims_by_email)} pilgrims ' +
            'with distinct email addresses!')
        stats = Stats()
        email_chunks = list(get_chunks(pilgrims_by_email.keys(), 20))
        for idx, email_chunk in enumerate(email_chunks):
            logger.info(
                f'Processing batch {idx+1} of {len(email_chunks)} batches')
            populate_pilgrim_data_tasks = []
            for email in email_chunk:
                pilgrims = pilgrims_by_email[email]
                pilgrims.sort(key=lambda pilgrim: int(pilgrim['pilgrim_id']))
                pilgrim = pilgrims[0]
                populate_pilgrim_data_tasks.append(
                    asyncio.create_task(
                        populate_additional_pilgrim_data(session, pilgrim)))
            update_subscriber_tasks = []
            for task in asyncio.as_completed(populate_pilgrim_data_tasks):
                pilgrim = await task
                pilgrim['group_ids'] = set()
                for newsletter in pilgrim['newsletters']:
                    title = newsletter['newsletter_label']
                    group_id = newsletter_group_ids_by_title[title]
                    pilgrim['group_ids'].add(group_id)
                logger.info(
                    'Found %s newsletter groups for pilgrim id %s',
                    len(pilgrim['group_ids']),
                    pilgrim['pilgrim_id'])
                update_subscriber_tasks.append(
                    asyncio.create_task(
                        update_subscriber(
                            stats, session, pilgrim, field_name_by_title)))
            for task in asyncio.as_completed(update_subscriber_tasks):
                pilgrim, result = await task
                logger.info(
                    'Finished processing subscriber "%s" with result "%s"',
                    pilgrim['email'],
                    result)
        logger.info(
            "Results: %s created, %s updated, %s no updates, %s errors",
            stats.created.value,
            stats.updated.value,
            stats.not_updated.value,
            stats.errors.value)


asyncio.run(main())
