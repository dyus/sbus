import asyncio
import logging
import math
import time
import typing
import uuid
from functools import partial

import pydantic
from aio_pika import (
    Channel, Connection, DeliveryMode, Exchange, ExchangeType, IncomingMessage,
    Message, connect_robust
)

from sbus.exceptions import RecoverableErrorBase, from_code_exception
from sbus.models import Context, ErrorResponseBody, Headers, Response
from sbus.subscribers import AbstractSubscriber

from ..serializer import JSONSerializer
from ..transports.base import AbstractTransport

logger = logging.getLogger(__name__)


def smart_int(value: str, default=None) -> typing.Optional[int]:
    try:
        return int(float(value))
    except (ValueError, TypeError, OverflowError):
        return default


class RPC:
    default_expiration = 10000

    def __init__(self, channel: Channel):
        self.channel = channel
        self.loop = self.channel.loop
        self.result_queue = None
        self.futures = {}
        self.result_consumer_tag = None
        self.routes = {}
        self.queues = {}
        self.consumer_tags = {}
        self.dlx_exchange = None

    def create_future(self) -> asyncio.Future:
        future = asyncio.get_event_loop().create_future()
        future_id = id(future)
        self.futures[future_id] = future
        future.add_done_callback(lambda f: self.futures.pop(future_id, None))
        return future

    def close(self) -> asyncio.Task:
        async def closer():
            nonlocal self

            if self.result_queue is None:
                return

            for future in self.futures.values():
                future.set_exception(asyncio.CancelledError)

            await self.result_queue.cancel(self.result_consumer_tag)
            self.result_consumer_tag = None

            await self.result_queue.delete()
            self.result_queue = None

        return self.loop.create_task(closer())

    @asyncio.coroutine
    def initialize(self, **kwargs):
        if self.result_queue is not None:
            return

        self.result_queue = yield from self.channel.declare_queue(None, **kwargs)

        self.result_consumer_tag = yield from self.result_queue.consume(
            self.on_result_message, exclusive=True, no_ack=True
        )

    @classmethod
    async def create(cls, channel: Channel, **kwargs) -> 'RPC':
        rpc = cls(channel)
        await rpc.initialize(**kwargs)
        return rpc

    async def on_result_message(self, message: IncomingMessage):
        correlation_id: typing.Optional[int] = (int(message.correlation_id)
                                                if message.correlation_id else None)
        future: asyncio.Future = self.futures.pop(correlation_id, None)

        if future is None:
            logger.warning('Unknown message: %r', message)
            return

        future.set_result(message.body)

    async def call(self, method_name: str, message: Message):
        future = self.create_future()

        await self.channel.default_exchange.publish(
            message=Message(
                body=message.body,
                type='call',
                reply_to=self.result_queue.name,
                delivery_mode=message.delivery_mode,
                message_id=message.message_id,
                correlation_id=id(future),
                expiration=message.expiration,
                headers=message.headers
            ),
            routing_key=method_name,
        )

        resp = await asyncio.wait_for(future, self.default_expiration)
        return resp

    async def on_call_message(self, method_name: str, message: IncomingMessage, context: Context,
                              subscriber: AbstractSubscriber, serializer: JSONSerializer):  # noqa
        if method_name not in subscriber.routing_keys:
            logger.warning('Method %r not registered in %r', method_name, self)
            return

        result = await subscriber.on_message(
            message.body,
            message.routing_key,
            context,
            serializer
        )

        result_message = Message(
            result,
            delivery_mode=message.delivery_mode,
            correlation_id=message.correlation_id,
            timestamp=int(time.time()),
        )

        await self.channel.default_exchange.publish(
            result_message,
            message.reply_to,
            mandatory=False
        )


class AioPikaTransport(AbstractTransport):
    default_command_retries = 10
    serializer = JSONSerializer()

    def __init__(self, connection_parameters, exchange_name, exchange_type, loop):
        self.connection_parameters = connection_parameters
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.connection: Connection = None
        self.channel: Channel = None
        self.exchange: Exchange = None
        self.rpc: RPC = None
        self.subscriber: AbstractSubscriber = None
        self.dlx_exchange = None

    async def connect(self):
        self.connection = await connect_robust(**self.connection_parameters)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
        )
        self.dlx_exchange = await self.channel.declare_exchange(
            'retries', type=ExchangeType.FANOUT
        )
        queue = await self.channel.declare_queue(
            'retries', durable=True, arguments={'x-dead-letter-exchange': self.exchange.name}
        )
        await queue.bind(self.dlx_exchange, '#')

        self.rpc = await RPC.create(self.channel)

    async def send(self, data: pydantic.BaseModel, routing_key: str, context: Context = None,
                   response_class=None):
        serialized_data = self.serializer.serialize(Response(body=data))
        corr_id = context.correlation_id or str(uuid.uuid4())

        if context.max_retries:
            retries = context.max_retries
        elif response_class:
            retries = self.default_command_retries
        else:
            retries = 0

        headers = {
            Headers.correlation_id: corr_id,
            Headers.retry_attempts_max: retries,
            Headers.expired_at: int(context.timeout + time.time()) if context.timeout else None,
            Headers.timestamp: int(time.time())
        }

        message = Message(
            body=serialized_data,
            message_id=context.message_id or str(uuid.uuid4()),
            expiration=context.timeout or self.rpc.default_expiration if response_class else None,
            headers={k: v for k, v in headers.items() if v is not None},
            delivery_mode=(DeliveryMode.NOT_PERSISTENT
                           if response_class else DeliveryMode.PERSISTENT)
        )
        logger.info('sbus ~~~> %s: %s', routing_key, serialized_data[:256])

        if response_class:
            res = await self.rpc.call(routing_key, message)
            logger.info('sbus resp <~~~ %s: %s', routing_key, res[:256])
            deserialized: Response = self.serializer.deserialize(res)

            if deserialized.status < 400:
                return response_class(**deserialized.body)
            else:
                err_message = ErrorResponseBody(**deserialized.body)
                err = from_code_exception[deserialized.status]
                raise err(err_message.error)
        else:
            return await self.exchange.publish(message, routing_key)

    async def on_message(self, subscriber: AbstractSubscriber, message: IncomingMessage):
        try:
            context_data = {
                Headers.message_id: message.message_id or str(uuid.uuid4()),
                Headers.routing_key: message.routing_key,
            }
            context_data.update(**message.headers or {})
            context = Context(**context_data)

            if message.type == 'call':
                result = await self.rpc.on_call_message(message.routing_key, message, context,
                                                        subscriber, self.serializer)
            else:
                logger.info('sbus <~~~ %s: %s', message.routing_key, message.body[:256])
                result = await subscriber.on_message(message.body, message.routing_key, context,
                                                     serializer=self.serializer)

            message.ack()
            return result

        except Exception as error:
            await self._handle_exception(message, error)

    async def subscribe(self, subscriber: AbstractSubscriber):
        for routing_key in subscriber.routing_keys:
            queue = await self.channel.declare_queue(routing_key, durable=subscriber.durable)
            await queue.bind(self.exchange, routing_key)
            await queue.consume(partial(self.on_message, subscriber))

    async def destroy(self):
        await self.rpc.close()
        await self.connection.close()

    async def _handle_exception(self, message: IncomingMessage, error: Exception):
        if not isinstance(error, RecoverableErrorBase):
            await self._on_fail(message, error)

        else:
            logger.exception(
                'Cannot process message "%s" in "%s"!. Because of %s.',
                message.delivery_tag,
                message.routing_key,
                error
            )

            headers: typing.Dict = message.headers or {}

            attempts_max: typing.AnyStr = smart_int(headers.get(Headers.retry_attempts_max))
            attempts_nr: typing.AnyStr = smart_int(headers.get(Headers.retry_attempt_nr), 1)

            if attempts_max and attempts_max > attempts_nr:
                headers[Headers.retry_attempt_nr] = str(attempts_nr + 1)

                backoff: int = int(math.pow(2, min(attempts_nr - 1, 7)))

                new_message: Message = Message(message.body,
                                               type=message.type,
                                               reply_to=message.reply_to,
                                               delivery_mode=message.delivery_mode,
                                               message_id=message.message_id,
                                               correlation_id=message.correlation_id,
                                               expiration=backoff,
                                               headers=headers)

                expires_at = headers.get(Headers.expired_at)
                if expires_at and int(expires_at) <= (time.time() + backoff):
                    logger.exception(
                        'timeout %s. Message will be expired %s, '
                        "don't retry it! Correlation id: %s",
                        message.routing_key,
                        expires_at,
                        headers.get(Headers.correlation_id),
                    )
                    await self._on_fail(message, error)

                else:
                    try:
                        logger.exception(
                            'Routing key: %s. Retry attempt %s after %s seconds...',
                            message.routing_key, attempts_nr, backoff
                        )
                        await self.dlx_exchange.publish(
                            new_message, routing_key=message.routing_key, mandatory=False
                        )
                    except Exception:  # noqa
                        logger.exception('Error on publish retry message for %s',
                                         message.routing_key)
                        await self._on_fail(message, error)
            else:
                logger.exception(
                    'max retries (%s) reached %s, '
                    "don't retry it! Correlation id: %s",
                    attempts_max,
                    message.routing_key,
                    headers.get(Headers.correlation_id),
                )

                message.nack(requeue=False)

    async def _on_fail(self, message: IncomingMessage, error: Exception):
        try:
            if message.reply_to:
                response = self.serializer.serialize(
                    Response(
                        status=getattr(error, 'status', 500),
                        body=ErrorResponseBody(error=str(error)).dict()
                    )
                )

                await self.channel.default_exchange.publish(
                    Message(
                        response,
                        delivery_mode=message.delivery_mode,
                        correlation_id=message.correlation_id,
                        timestamp=int(time.time()),
                    ),
                    message.reply_to,
                    mandatory=False
                )

        finally:
            logger.exception(
                'Invalid message (%s) with routing_key %s, '
                "don't retry it! Correlation id: %s",
                message.body,
                message.routing_key,
                message.headers.get(Headers.correlation_id) if message.headers else None,
            )
            message.nack(requeue=False)
