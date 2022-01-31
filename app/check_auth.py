from helpers.clientsession import get_client_session
import logging
import asyncio
import helpers.directory as directory
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    async with get_client_session() as session:
        # fetch weeks and conferences
        auth = await directory.get_auth(session)
        logger.info(
            f'Auth: \n{auth}')


asyncio.run(main())
