from aiohttp import ClientError
from helpers.clientsession import get_client_session
import logging
import asyncio
from dotenv import load_dotenv
import helpers.directory as directory

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
LOCATION_TO_CONFERENCE = {
    'Camp Weaver': PIEDMONT_CONFERENCE,
    'Camp Hanes': PIEDMONT_CONFERENCE,
    'Camp New Hope': PIEDMONT_CONFERENCE,
    'Laurel Ridge Moravian Conference Center': PIEDMONT_CONFERENCE,
    'Camp Dogwood': PIEDMONT_CONFERENCE,
    'Camp Harrison': WESTERN_CONFERENCE,
    'Camp Dixie': EASTERN_CONFERENCE
}


async def update_week(session, week, conf_id_by_name):
    week_id = week['week_id']
    location = week['location']
    result = 'Unknown'
    if(location):
        conference = LOCATION_TO_CONFERENCE[location]
        if(conference):
            conf_id = conf_id_by_name[conference]
            if(conf_id):
                week['conference_id'] = conf_id
                try:
                    await directory.update_week(session, week_id, week)
                    result = (
                        f'Updated week #{week_id} '
                        f'to conference "{conference}"')
                except ClientError as error:
                    result = (
                        f'Updated for week #{week_id} failed!'
                        f' Response status: {error.status}')
            else:
                result = (
                    f'Skipped update of week #{week_id}: '
                    'no conference id found for '
                    f'conference named "{conference}"')
        else:
            result = (
                f'Skipped update of week #{week_id}: '
                'no conference mapped to '
                f'location "{location}"')
    else:
        result = (
            f'Skipped update of week #{week_id}: '
            'week does not have a location!')
    return result


async def main():
    async with get_client_session() as session:
        # fetch weeks and conferences
        weeks, conferences = await asyncio.gather(
            directory.get_weeks(session),
            directory.get_conferences(session)
        )
        logger.info(
            f'Found {len(weeks)} weeks and {len(conferences)} conferences')
        conf_id_by_name = {}
        for conf in conferences:
            conf_id_by_name[conf['conference_name']] = conf['conference_id']
        update_tasks = []
        for week in weeks:
            update_tasks.append(
                asyncio.create_task(
                    update_week(session, week, conf_id_by_name)))
        for task in asyncio.as_completed(update_tasks):
            result = await task
            logger.info(result)


asyncio.run(main())
