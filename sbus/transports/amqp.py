"""
Use this module members to connect to RabbitMQ instance.
"""

import asyncio
import logging
import math
import time
import typing  # noqa
from uuid import uuid4

import aioamqp
from aioamqp import AmqpClosedConnection, ChannelClosed
from aioamqp.channel import Channel  # noqa
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties
from aioamqp.protocol import CLOSED

from .. import exceptions
from ..models import Context, Headers, Response, ResponseBody
from ..serializer import JSONSerializer
from ..subscribers import AbstractSubscriber
from .base import AbstractTransport

logger = logging.getLogger(__name__)


def smart_int(value: str, default=None) -> typing.Optional[int]:
    """Cast str to int or return default value"""

    try:
        return int(float(value))
    except (ValueError, TypeError, OverflowError):
        return default


class AMQPTransport(AbstractTransport):
    """
    Handy implementation of the asynchronous AMQP client.
    Useful for building both publishers and consumers.
    """

    def __init__(
            self,
            connection_parameters,
            exchange_name,
            exchange_type,
            channel_name,
            connection_timeout=3,
            delivery_mode=2,
            loop=None,
            serializer=None,
    ):
        """There must be at least these members of the connection_parameters dict:
            'connection_parameters': {
                'login': '',
                'password': '',
                'host': '',
                'port': '',
                'virtualhost': '',
            },


        :param connection_parameters: Dict with connection parameters. See above for its format.
        :return: EventsQueueClient instance.
        """

        # Can not pass empty password when connecting. Must remove the field completely.
        if not connection_parameters.get('password', ''):
            connection_parameters.pop('password', None)

        self._connection_parameters = connection_parameters
        self._transport = None
        self._protocol = None
        self._channel: Channel = None
        self.channel_name = channel_name
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._delivery_mode = delivery_mode
        self._connection_timeout = connection_timeout
        self._is_connecting = False
        self._connected = False
        self._connection_guid = str(uuid4())
        self._known_queues = {}
        self._routing = {}
        self.loop = loop
        self._serializer = serializer or JSONSerializer()

        # TODO move to config
        self.retry_exchange = 'retries'
        self.default_command_retries = 100
        self.default_timeout = 12

    @property
    def routing(self):
        return self._routing

    @property
    def known_queues(self):
        return self._known_queues

    @property
    def connected(self):
        return self._connected or (self._channel is not None and self._channel.is_open)

    @property
    def is_connecting(self):
        return self._is_connecting

    @property
    def channel(self):
        return self._channel

    @property
    def transport(self):
        return self._transport

    @property
    def protocol(self) -> aioamqp.AmqpProtocol:
        return self._protocol

    async def connect(self):
        """
        Create new asynchronous connection to the RabbitMQ instance.
        This will connect, declare exchange and bind itself to the configured queue.

        After that, client is ready to publish or consume messages.

        :return: Does not return anything.
        """
        if self.connected or self.is_connecting:
            return

        self._is_connecting = True
        try:
            logger.info(
                'Connecting to RabbitMQ on %s/%s...',
                self._connection_parameters.get('host'),
                self._connection_parameters.get('virtualhost')
            )
            self._transport, self._protocol = await asyncio.wait_for(
                aioamqp.connect(
                    loop=self.loop,
                    on_error=self.callback_error,
                    **self._connection_parameters
                ),
                self._connection_timeout
            )
            self._channel = await self._protocol.channel()
            self._connected = True
        except (AmqpClosedConnection,
                ChannelClosed,
                ConnectionRefusedError,
                asyncio.CancelledError) as err:

            self._is_connecting = False
            logger.exception('RabbitMQ connection error')
            self.callback_error(err)

        self._is_connecting = False
        return self._transport, self._protocol

    def callback_error(self, exception):
        raise exceptions.StreamConnectionError('Error initializing RabbitMQ connection') \
            from exception

    async def command(self, data, routing_key):
        if not self.connected:
            logger.warning('Attempted to send message while not connected')
            return

        body = self._serializer.serialize(Response(body=data))

        # TODO use context!
        default_properties = {
            'delivery_mode': self._delivery_mode,
            'headers': {
                Headers.correlation_id: self.generate_correlation_id(),
                Headers.retry_attempts_max: str(self.default_command_retries),
                Headers.expired_at: str(self.default_timeout),
            }
        }

        logger.info('sbus <~~~ %s: %s', routing_key, body[:256])

        await self.channel.publish(
            payload=body,
            exchange_name=self._exchange_name,
            routing_key=routing_key,
            properties=default_properties,
        )

    async def on(self, subscriber: AbstractSubscriber):
        """Subscribe to the queue consuming."""
        await self.declare_queue(subscriber.name, subscriber.durable)

        for routing_key in subscriber.routing_keys:
            await self.bind_to_queue(routing_key, subscriber.name)

        for key in subscriber.routing_keys:
            if subscriber == self._routing.get(key):
                logger.warning(
                    'Subscriber "%s" already received routing_key "%s".',
                    subscriber,
                    key
                )
                break
            self._routing[key] = subscriber

        logger.info('Starting consumption for %s', subscriber)

        await self._channel.basic_qos(prefetch_count=subscriber.prefetch_count)
        await self._channel.basic_consume(
            callback=self._on_message,
            consumer_tag=subscriber.consumer_tag(),
            queue_name=subscriber.name,
        )

        self._add_to_known_queues(subscriber)

    async def declare_queue(self, queue_name, durable=True):
        logger.info('Declaring queue...')
        queue_declaration = await self._channel.queue_declare(queue_name, durable=durable)
        queue_name = queue_declaration.get('queue')
        logger.info('Declared queue "%s"', queue_name)

    async def declare_exchange(self, exchange_name, type_name='direct', durable=True):
        logger.info('Declaring exchange...')
        await self._channel.exchange_declare(exchange_name, type_name=type_name, durable=durable)
        logger.info('Declared exchange "%s" with type "%s" and durable=%s', exchange_name,
                    type_name, durable)

    async def bind_to_queue(self, routing_key, queue_name):
        """Bind to queue with specified routing key."""
        logger.info('Binding exchange="%s", queue="%s", key="%s"',
                    self._exchange_name, queue_name, routing_key)

        result = await self._channel.queue_bind(
            exchange_name=self._exchange_name,
            queue_name=queue_name,
            routing_key=routing_key,
        )
        return result

    async def _on_message(self, channel, body, envelope, properties):
        """Subscriber messages handler."""
        try:
            logger.debug('Processing message correlation_id=%s', properties.correlation_id)
            logger.info('sbus ~~~> %s: %s', envelope.routing_key, body[:256])

            subscriber: AbstractSubscriber = self._routing.get(envelope.routing_key)
            if not subscriber:
                logger.debug('No route for message with key "%s"', envelope.routing_key)
                return

            context = Context(message_id=properties.message_id, routing_key=envelope.routing_key)

            response_body = await subscriber.on_message(
                body, envelope.routing_key, context, self._serializer
            )

            if not properties.reply_to:
                await self.ack(envelope.delivery_tag)
                return

            else:
                # TODO log correlation id as additional field
                # TODO move to config body slice
                logger.info('sbus resp <~~~ %s: %s', envelope.routing_key, response_body[:256])

                await self.channel.publish(
                    exchange_name='',
                    routing_key=properties.reply_to,
                    properties={
                        'correlation_id': properties.correlation_id,
                    },
                    payload=response_body
                )

            await self.ack(envelope.delivery_tag)

        except exceptions.UnrecoverableExceptionBase as error:
            await self._handle_unrecoverable_exception(envelope, properties, error)

        # any exception which not marked as unrecoverable will be retried
        except Exception as error:
            await self._handle_recoverable_exception(envelope, body, properties, error)

    async def _handle_unrecoverable_exception(self, envelope: Envelope, properties: Properties,
                                              error: exceptions.UnrecoverableExceptionBase):
        logger.error(
            'Cannot process message "%s" in "%s" because of %s.',
            envelope.delivery_tag,
            envelope.routing_key,
            error
        )
        await self._on_fail(envelope, properties, error)

    async def _handle_recoverable_exception(self, envelope: Envelope,
                                            payload: bytes, properties: Properties,
                                            error: Exception):
        logger.exception(
            'Cannot process message "%s" in "%s"!. Because of %s.',
            envelope.delivery_tag,
            envelope.routing_key,
            error
        )

        headers: typing.Dict = properties.headers or {}

        # TODO aioamqp can not publish x-death header because it's type is list
        # right now i do not need lists in headers but this should be workaround somehow
        headers.pop('x-death', None)

        attempts_max: typing.AnyStr = smart_int(headers.get(Headers.retry_attempts_max))
        attempts_nr: typing.AnyStr = smart_int(headers.get(Headers.retry_attempt_nr), 1)

        if attempts_max and attempts_max > attempts_nr:
            headers[Headers.retry_attempt_nr] = str(attempts_nr + 1)

            backoff: int = int(math.pow(2, min(attempts_nr - 1, 7))) * 1000

            new_properties = {
                'delivery_mode': 2,
                'headers': headers,
                'expiration': str(backoff),
            }

            expires_at = headers.get(Headers.expired_at)
            if expires_at and float(expires_at) <= (time.time() * 1000 + backoff):
                logger.exception(
                    'timeout %s. Message will be expired %s, '
                    'don\'t retry it! Correlation id: %s',
                    envelope.routing_key,
                    expires_at,
                    headers.get(Headers.correlation_id),
                )
                await self._on_fail(envelope, properties, error)

            else:
                try:
                    logger.exception(
                        'Routing key: %s. Retry attempt %s after %s millis...',
                        envelope.routing_key, attempts_nr, backoff
                    )
                    await self.channel.publish(
                        exchange_name=self.retry_exchange,
                        routing_key=envelope.routing_key,
                        properties=new_properties,
                        payload=payload
                    )
                except Exception:  # noqa
                    logger.exception('Error on publish retry message for %s',
                                     envelope.routing_key)
                finally:
                    await self._on_fail(envelope, properties, error)
        else:
            logger.exception(
                'max retries (%s) reached %s, '
                'don\'t retry it! Correlation id: %s',
                attempts_max,
                envelope.routing_key,
                headers.get(Headers.correlation_id),
            )

            await self.nack(envelope.delivery_tag)

    async def _on_fail(self, envelope: Envelope, properties: Properties, error: Exception):
        """ Call when we have no chance to process message."""
        try:
            if properties.reply_to:
                response = self._serializer.serialize(
                    Response(
                        status=getattr(error, 'status', 500),
                        body=ResponseBody(error=str(error)).dict()
                    )
                )

                logger.info('sbus resp <~~~ %s: %s', envelope.routing_key, response[:256])

                await self.channel.publish(
                    exchange_name='',
                    routing_key=properties.reply_to,
                    properties={'correlation_id': properties.correlation_id},
                    payload=response
                )

        finally:
            await self.nack(envelope.delivery_tag)

    def _add_to_known_queues(self, subscriber: AbstractSubscriber):
        self._known_queues[subscriber.name] = subscriber

    @staticmethod
    def generate_correlation_id():
        corr_id = str(uuid4())
        logger.debug('Generated correlation_id %s', corr_id)
        return corr_id

    async def _declare_response_queue(self):
        logger.info('Declaring rpc queue...')
        queue_declaration = await self.channel.queue_declare(exclusive=True)
        queue_name = queue_declaration.get('queue')
        logger.info('Declared rpc queue "%s"', queue_name)
        return queue_name

    async def request(self, data, routing_key, callback=None):
        """RPC call method

        :param data: query data
        :param routing_key: routing key
        :param callback: optional callback to execute when PRC-call is completed
        """
        if not self.connected:
            logger.warning('Attempted to send message while not connected')
            return

        corr_id = self.generate_correlation_id()
        callback_queue = await self._declare_response_queue()

        body = self._serializer.serialize(Response(body=data))
        waiter = asyncio.Event()

        def waiter_callback(ch, body, method, props):
            try:
                if callback:
                    callback(ch, body, method, props)
            finally:
                waiter.set()

        logger.debug('Start callback consuming on queue: %s', callback_queue)
        await self.channel.basic_consume(waiter_callback, callback_queue, no_ack=True)

        logger.debug('Publish RPC call data: %s, exchange_name: %s, routing_key: %s, '
                     'reply_to: %s, correlation_id:%s, delivery_mode: %s', data,
                     self._exchange_name, routing_key, callback_queue, corr_id,
                     self._delivery_mode)

        await self.channel.publish(
            exchange_name=self._exchange_name,
            routing_key=routing_key,
            properties={
                'reply_to': callback_queue,
                'correlation_id': corr_id,
                'headers': {
                    Headers.correlation_id: self.generate_correlation_id(),
                    Headers.retry_attempts_max: self.default_command_retries,
                    Headers.expired_at: str(self.default_timeout),
                }
            },
            payload=body
        )

        await waiter.wait()

    async def ack(self, delivery_tag):
        await self._channel.basic_client_ack(delivery_tag)
        logger.debug('Message "%s" was acked.', delivery_tag)

    async def nack(self, delivery_tag):
        await self._channel.basic_client_nack(delivery_tag, requeue=False)
        logger.debug('Message "%s" was negative acked.', delivery_tag)

    async def destroy(self, *args, **kwargs):
        # cancel consuming
        for subscriber in self._known_queues.values():
            logger.info('Cancelling consumption for %s', subscriber)
            await self._channel.basic_cancel(subscriber.consumer_tag())

        if self._channel and self._channel.is_open:
            await self._channel.close()

        if self._protocol is not None and self._protocol.state != CLOSED:
            # wait till all heartbeats is stopped
            wait = kwargs.get('wait', False)

            try:
                # close using the `AMQP` protocol
                self.protocol.connection_lost(exc=None)
                await self.protocol.close(no_wait=not wait, timeout=10)

                # ensure the socket is closed.
                self.transport.close()
            except (aioamqp.AmqpClosedConnection, exceptions.StreamConnectionError):
                logger.info('Graceful connection shutdown detected')

        self._connected = False
        self._is_connecting = False
