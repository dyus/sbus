import asyncio
import logging
import typing

from .subscribers import AbstractSubscriber
from .transports.base import AbstractTransport
from .utils import create_transport

logger = logging.getLogger(__name__)


class Sbus:

    def __init__(self, transport: AbstractTransport, loop=None):
        """
        :param transport: broker transport
        :param loop: event loop instance
        """
        self.transport = transport
        self.subscribers = set()
        self.loop = loop or asyncio.get_event_loop()

    async def connect(self):
        await self.transport.connect()

    @property
    def connected(self):
        return self.transport.connected

    async def command(self, message, routing_key):
        """Publish message with chosen transport

        :param message: message for publish
        :param routing_key: routing key for message
        """
        await self.transport.command(message, routing_key)

    async def request(self, message, routing_key, callback=None):
        """RPC call with data and routing key

        :param message:  message for publish
        :param routing_key: routing key for message
        :param callback: callback for returned message
        """
        await self.transport.request(message, routing_key, callback)

    async def on(self, subscriber: AbstractSubscriber):
        """Subscribe on queue."""
        if not self.connected:
            return

        if subscriber in self.subscribers:
            logger.warning('Duplicate subscriber %s', subscriber)
            return

        await self.transport.on(subscriber)
        self.subscribers.add(subscriber)

    async def stop(self):
        await self.transport.destroy()
        self.subscribers.clear()

    def clone_subscribers(self):
        return self.subscribers.copy()

    @classmethod
    def create_factory(cls, config, loop):
        loop = loop or asyncio.get_event_loop()

        def _factory():
            transport = create_transport(config, loop=loop)
            return cls(transport, loop)

        return _factory


class BackOff:
    """Infinite backoff generation"""

    def __init__(self, min_timeout=1, max_timeout=15):
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self.retry_count = 0

    def get_timeout(self):
        self.retry_count += 1
        timeout = self.min_timeout * self.retry_count
        return timeout if timeout < self.max_timeout else self.max_timeout

    def reset(self):
        self.retry_count = 0


class ServiceBusWatchDog:
    """Class for control Sbus health and status.

    This class should be used if you want survive through broker downs.
    """
    def __init__(self, service_bus_factory: typing.Callable, *, backoff: BackOff=None):
        """
        :param service_bus_factory: callable, must return Sbus instance
        :param backoff: BackOff instance to handle reconnection timeouts
        """
        self._service_bus_factory = service_bus_factory
        self._lock = asyncio.Lock()
        self._retry_attempts = 0
        self._backoff = backoff or BackOff()
        self._service_bus: Sbus = self._service_bus_factory()

    @property
    def sbus(self) -> Sbus:
        return self._service_bus

    async def repair(self):

        if self._lock.locked():
            logger.warning('Service bus repairing is in progress now')
            return

        with await self._lock:
            await self._repair()

    async def _repair(self):
        logger.error('Closed connection detected. Trying to reconnect to service bus.')
        backoff_timeout = self._backoff.get_timeout()

        logger.info('Reconnection attempts=%s, next in %s sec',
                    self._backoff.retry_count, backoff_timeout)

        await asyncio.sleep(backoff_timeout)

        current_service_bus: Sbus = self._service_bus
        loop = current_service_bus.loop

        subscribers = current_service_bus.clone_subscribers()

        try:
            await current_service_bus.stop()
            del current_service_bus
        except Exception:
            logger.exception('Exception occurred when stopping service bus, but may be ignored')

        try:
            sbus = self._service_bus_factory()

            await sbus.connect()
            await asyncio.gather(
                *[sbus.on(subscriber) for subscriber in subscribers],
                loop=sbus.loop
            )

            self._service_bus = sbus
            self._backoff.reset()
        except Exception:
            # several exceptions possible here, but we may safely ignore them
            logger.exception('Exception occurred when reconnecting, but may be ignored')
            loop.create_task(self.repair())
            return False

        logger.info('Repair is completed successfully')
        return True
