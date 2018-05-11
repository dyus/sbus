import json

import pytest
from aioamqp.channel import Channel

from service_bus.exceptions import StreamConnectionError
from service_bus.transports import AMQPTransport
from tests.utils import make_mocked_coro


class TestAMQPTransport:
    @pytest.fixture
    def setup_queue(self, event_loop, amqp_client, exchange_name, queue_name,
                    test_topic):
        event_loop.run_until_complete(amqp_client.declare_queue(queue_name))
        event_loop.run_until_complete(amqp_client.bind_to_queue(test_topic, queue_name))

    @pytest.fixture
    def not_connected_client(self, connection_parameters, exchange_name, exchange_type,
                             worker_name, event_loop):
        return AMQPTransport(connection_parameters, exchange_name, exchange_type, worker_name,
                             loop=event_loop)

    def test_amqp_client_init_and_connect(
        self, amqp_client, connection_parameters, exchange_name, exchange_type, worker_name,
        event_loop,
    ):
        """Test initial state after connect."""
        assert amqp_client._connection_parameters == connection_parameters
        assert amqp_client._transport
        assert amqp_client._protocol
        assert amqp_client._channel
        assert amqp_client._serializer
        assert amqp_client.channel_name == worker_name
        assert amqp_client._exchange_name == exchange_name
        assert amqp_client._exchange_type == exchange_type
        assert amqp_client._delivery_mode == 2
        assert amqp_client._connection_timeout == 3
        assert not amqp_client._is_connecting
        assert amqp_client._connection_guid
        assert amqp_client._known_queues == {}
        assert amqp_client._routing == {}
        assert amqp_client.loop == event_loop
        assert amqp_client.routing == {}
        assert amqp_client.known_queues == {}
        assert amqp_client.connected is True
        assert not amqp_client.is_connecting
        assert isinstance(amqp_client.channel, Channel)
        assert amqp_client.transport
        assert amqp_client.protocol

    @pytest.mark.asyncio
    async def test_callback_error(self, ctx, not_connected_client):
        not_connected_client.connect = make_mocked_coro(raise_exception=StreamConnectionError)
        with pytest.raises(StreamConnectionError):
            await not_connected_client.connect()

    @pytest.mark.asyncio
    async def test_publish(self, ctx, setup_queue, amqp_client,
                           echo_subscriber, test_topic):
        assert amqp_client.connected
        message = {'test': 'hello'}
        await amqp_client.publish(message, test_topic)

        on_message_queue = ctx.create_queue(size=1, timeout=3)

        async def on_message(data, routing_key, context_, serializer_):
            on_message_queue.put_nowait((data, routing_key, context_, serializer_))

        echo_subscriber.on_message = on_message
        await amqp_client.subscribe(echo_subscriber)
        r_message, r_key, r_context, r_serializer = await on_message_queue.get()
        assert (json.loads(r_message).get('body'), r_key) == (message, test_topic)

    @pytest.mark.asyncio
    async def test_client_destroy(self, ctx, not_connected_client):
        """
        Test client destroyable.
        """
        await not_connected_client.connect()
        await not_connected_client.destroy()

    @pytest.mark.asyncio
    async def test_consume_queue(self, ctx, setup_queue, amqp_client, subscriber):
        """
        Test that can subscribe.
        """
        await amqp_client.subscribe(subscriber)

        assert subscriber.routing_keys == tuple(amqp_client._routing.keys())
        assert subscriber in list(amqp_client._routing.values())
        assert subscriber.name in amqp_client._known_queues
