from helpers.clientsession import get_client_session
import logging
import re
import json
import asyncio
from dotenv import load_dotenv
import helpers.directory as directory
from helpers.chunker import get_chunks

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PIEDMONT_CONFERENCE = 'Piedmont'
WESTERN_CONFERENCE = 'Western'
EASTERN_CONFERENCE = 'Eastern'
CONFERENCES = {
    PIEDMONT_CONFERENCE,
    WESTERN_CONFERENCE,
    EASTERN_CONFERENCE
}

CONF_NEWSL_REGEX = r'^(Piedmont|Western|Eastern) Conference$'


async def update_pilgim_conf_subscriptions(
        session, pilgrim, newsletter_ids_by_conf_name):
    pilgrim_id = pilgrim['pilgrim_id']
    logger.info(
        f'Fetching roles and newsletters for pilgrim id {pilgrim_id}...')
    roles, pilg_newsletters = await asyncio.gather(
        directory.get_pilgrim_roles(session, pilgrim_id),
        directory.get_pilgrim_newsletters(session, pilgrim_id)
    )
    logger.info(
        f'Fetched {len(roles)} role(s) and ' +
        f'{len(pilg_newsletters)} newsletter(s) for ' +
        f'pilgrim id {pilgrim_id}...')
    conf_names = set()
    for role in roles:
        conf_names.add(role['conference_name'])
    cur_newsl_ids = set()
    for pilg_newsletter in pilg_newsletters:
        cur_newsl_ids.add(pilg_newsletter['newsletter_id'])
    newsletter_ids = []
    for conf_name in conf_names:
        if(conf_name in newsletter_ids_by_conf_name):
            newsletter_id = newsletter_ids_by_conf_name[conf_name]
            if(newsletter_id not in cur_newsl_ids):
                newsletter_ids.append(newsletter_id)
    if(len(newsletter_ids)):
        logger.info((
            f'Adding pilgrim id {pilgrim_id} to newsletters: ' +
            json.dumps(newsletter_ids)
        ))
        await directory.add_pilgrim_newsletters(
            session, pilgrim_id, newsletter_ids)
        return (
            f'Added pilgrim id {pilgrim_id} to newsletters: ' +
            json.dumps(newsletter_ids)
        )
    else:
        return f'Skipping pilgrim id {pilgrim_id}, no newsletters to add'


async def main():
    async with get_client_session() as session:
        pilgrims, newsletters = await asyncio.gather(
            directory.get_pilgrims(session),
            directory.get_newsletters(session)
        )
        logger.info(
            f'Fetched {len(pilgrims)} pilgrims(s) and ' +
            f'{len(newsletters)} newsletter(s)')
        newsletter_ids_by_conf_name = {}
        for newsletter in newsletters:
            results = re.match(
                CONF_NEWSL_REGEX, newsletter['newsletter_label'])
            if(results):
                newsletter_ids_by_conf_name[results.group(1)] = \
                    newsletter['newsletter_id']
        pilgrim_chunks = list(get_chunks(pilgrims, 20))
        for idx, pilgrim_chunk in enumerate(pilgrim_chunks):
            logger.info(
                f'Processing batch {idx+1} of {len(pilgrim_chunks)} batches')
            tasks = []
            for pilgrim in pilgrim_chunk:
                tasks.append(
                    asyncio.create_task(
                        update_pilgim_conf_subscriptions(
                            session, pilgrim, newsletter_ids_by_conf_name)
                    )
                )
            for task in asyncio.as_completed(tasks):
                result = await task
                logger.info(result)
    logger.info('Finished!')

asyncio.run(main())
