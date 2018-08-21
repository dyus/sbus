import asyncio
import logging

from .subscribers import AbstractSubscriber
from .transports.base import AbstractTransport
from .utils import create_transport

logger = logging.getLogger(__name__)


class Sbus:

    def __init__(self, transport: AbstractTransport, loop=None):
        self.transport = transport
        self.loop = loop or asyncio.get_event_loop()

    async def connect(self):
        await self.transport.connect()

    async def command(self, message, routing_key, context):
        return await self.transport.send(message, routing_key, context)

    async def event(self, message, routing_key, context):
        asyncio.ensure_future(self.transport.send(message, routing_key, context))

    async def request(self, message, routing_key, context, response_class):
        return await self.transport.send(message, routing_key, context, response_class)

    async def on(self, subscriber: AbstractSubscriber):
        await self.transport.subscribe(subscriber)

    async def stop(self):
        await self.transport.destroy()

    @classmethod
    def create_factory(cls, config, loop):
        loop = loop or asyncio.get_event_loop()

        def _factory():
            transport = create_transport(config, loop=loop)
            return cls(transport, loop)

        return _factory
