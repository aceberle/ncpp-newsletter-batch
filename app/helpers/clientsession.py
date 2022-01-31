import aiohttp
import asyncio
import logging
from types import SimpleNamespace
from aiohttp import (
    ClientSession,
    TraceConfig,
    TraceRequestStartParams,
    TCPConnector,
    ClientTimeout
)
from aiohttp_retry import RetryClient, ExponentialRetry  # type: ignore

logger = logging.getLogger(__name__)


def get_client_session(
        max_tries=3,
        max_time=30000,
        timeout=3000,
        connect_limit_per_host=2) -> ClientSession:
    async def _on_request_start(
        session: ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: TraceRequestStartParams
    ) -> None:
        current_attempt = \
            trace_config_ctx.trace_request_ctx['current_attempt']
        if(current_attempt > 1):
            logger.warning(
                f'::warn ::Retry Attempt #{current_attempt} ' +
                f'of {max_tries}: {params.method} {params.url}')
    trace_config = TraceConfig()
    trace_config.on_request_start.append(_on_request_start)
    limit_per_host = max(0, connect_limit_per_host)
    connector = TCPConnector(
        limit_per_host=limit_per_host,
        ttl_dns_cache=600  # 10-minute DNS cache
    )
    retry_options = ExponentialRetry(
                        attempts=max_tries,
                        max_timeout=max_time,
                        exceptions=[
                            aiohttp.ClientError,
                            asyncio.TimeoutError
                        ])
    return RetryClient(
            raise_for_status=True,
            connector=connector,
            timeout=ClientTimeout(total=timeout),
            retry_options=retry_options,
            trace_configs=[trace_config])
