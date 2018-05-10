import json
from unittest import mock

import pytest

from service_bus.service_bus import BackOff, ServiceBus, ServiceBusWatchDog
from service_bus.transports import SUPPORTED_TRANSPORTS, AMQPTransport
from service_bus.utils import create_transport
from tests.conftest import Context  # noqa
from tests.utils import make_mocked_coro


@pytest.fixture
def transport_config(connection_parameters, exchange_name, exchange_type, worker_name):
    return {
        'transports': {
            'amqp': {
                'connection_parameters': connection_parameters,
                'exchange_name': exchange_name,
                'exchange_type': exchange_type,
                'channel_name': worker_name,
                'delivery_mode': 2,
                'connection_timeout': 10,
            }
        },
        'active_transport': 'amqp',
    }


@pytest.fixture
def transport(transport_config, event_loop):
    return create_transport(transport_config, event_loop)


class TestServiceBus:

    @pytest.fixture
    def service_bus(self, event_loop, transport_config) -> ServiceBus:
        active_transport = transport_config['active_transport']
        active_transport_setup = transport_config['transports'][active_transport]
        transport_class = SUPPORTED_TRANSPORTS[active_transport]
        transport = transport_class(**active_transport_setup)
        return ServiceBus(transport, loop=event_loop)

    def test_service_bus_init(self, service_bus: ServiceBus):
        assert isinstance(service_bus.transport, AMQPTransport)
        assert isinstance(service_bus.subscribers, set)

    @pytest.mark.asyncio
    async def test_connect(self, service_bus: ServiceBus):
        mocked_connect = make_mocked_coro()
        service_bus.transport.connect = mocked_connect
        await service_bus.connect()
        assert mocked_connect.called

    def test_connected(self, service_bus: ServiceBus):
        mocker = mock.patch('tests.test_service_bus.AMQPTransport.connected',
                            new=mock.PropertyMock(return_value=True))
        with mocker as mocked_connected:
            service_bus.transport.connected = mocked_connected
            result = service_bus.connected
            assert result
            assert mocked_connected.called

    @pytest.mark.asyncio
    async def test_publish(self, service_bus: ServiceBus):
        mocked_publish = make_mocked_coro()
        service_bus.transport.publish = mocked_publish
        await service_bus.publish('message', 'routing_key')
        mocked_publish.assert_called_once()

    @pytest.fixture
    def callback(self):
        def func(ch, method, props, body):
            pass
        return func

    @pytest.mark.asyncio
    async def test_dequeue(self, service_bus: ServiceBus, subscriber):
        mocked_consume_queue = make_mocked_coro()
        service_bus.transport.subscribe = mocked_consume_queue
        with mock.patch('tests.test_service_bus.AMQPTransport.connected',
                        new=mock.PropertyMock(return_value=True)):
            await service_bus.subscribe(subscriber)
            assert mocked_consume_queue.called

    @pytest.mark.asyncio
    async def test_stop(self, service_bus: ServiceBus):
        mocked_destroy = make_mocked_coro()
        with mock.patch('tests.test_service_bus.AMQPTransport.destroy',
                        new=mocked_destroy):
            await service_bus.stop()
            assert mocked_destroy.called


class TestServiceBusWatchDog:

    def test_class_interface(self, transport_config, worker_name, connection_parameters):
        loop = mock.Mock()

        service_bus_factory = ServiceBus.create_factory(transport_config, loop)
        watch_dog = ServiceBusWatchDog(service_bus_factory)

        assert watch_dog.service_bus.loop == loop
        assert watch_dog.service_bus.transport.channel_name == worker_name
        assert watch_dog.service_bus.transport._connection_parameters == connection_parameters

    @pytest.mark.asyncio
    async def test_repair_connection(self, ctx: 'Context', transport_config, event_loop,
                                     echo_subscriber, amqp_client, test_topic):

        service_bus_factory = ServiceBus.create_factory(transport_config, event_loop)
        backoff = BackOff(max_timeout=10)
        watch_dog = ServiceBusWatchDog(service_bus_factory, backoff=backoff)

        on_message_queue = ctx.create_queue(size=1, timeout=10)

        async def on_message(data, routing_key, context_, serializer_):
            on_message_queue.put_nowait((data, routing_key, context_, serializer_))

        echo_subscriber.on_message = on_message

        await watch_dog.service_bus.connect()
        await watch_dog.service_bus.subscribe(echo_subscriber)

        assert watch_dog.service_bus.clone_subscribers() == {echo_subscriber}

        published_message = {'hello': 'world'}

        await amqp_client.publish(published_message, test_topic)

        r_message, r_key, r_context, r_serializer = await on_message_queue.get()
        assert (json.loads(r_message).get('body'), r_key) == (published_message, test_topic)

        # TODO: takes a lot of time to complete
        # await watch_dog.service_bus.transport.protocol.close(no_wait=False)

        # we must wait until connection is fully closed
        await watch_dog.repair()

        await amqp_client.publish(published_message, test_topic)

        r_message, r_key, r_context, r_serializer = await on_message_queue.get()
        assert (json.loads(r_message).get('body'), r_key) == (published_message, test_topic)

        await watch_dog.service_bus.stop()
