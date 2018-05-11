import asyncio
import logging
import os
import sys

import pytest

from sbus.router import Router
from sbus.subscribers import QueueSubscriber
from sbus.transports import AMQPTransport
from tests.utils import make_mocked_coro

logger = logging.getLogger(__name__)


class Context:

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self._on_shutdown = []

    def enable_logging(self, level=logging.DEBUG):
        current_level = logging.getLogger().getEffectiveLevel()
        logging.basicConfig(level=level, stream=sys.stderr)

        def _disable_logging():
            logging.basicConfig(level=current_level)

        self.add_on_shutdown(_disable_logging)

    def create_queue(self, size=0, timeout=None):
        queue = asyncio.Queue(maxsize=size, loop=self.loop)

        def _close_queue():
            if not queue._getters:
                return None

            # there are some waiters for messages,
            for getter in queue._getters:
                getter.set_result(None)

            if timeout:
                pytest.fail(f'Queue timeout "{timeout}" seconds has been reached')
            else:
                raise pytest.fail('Queue is not released until test shutdown')

        if timeout:
            self.loop.call_later(timeout, _close_queue)
        else:
            self.add_on_shutdown(_close_queue)

        return queue

    def create_event(self, timeout=None):
        event = asyncio.Event(loop=self.loop)

        def _set_event():
            if event.is_set():
                return
            event.set()
            if timeout:
                pytest.fail(f'Timeout "{timeout}" seconds has been reached')
            else:
                pytest.fail('Event was not set until test shutdown')

        if timeout:
            self.loop.call_later(timeout, _set_event)
        else:
            self.add_on_shutdown(_set_event)
        return event

    def add_on_shutdown(self, callback):
        self._on_shutdown.append(asyncio.coroutine(callback)())

    def shutdown(self):
        if self._on_shutdown:
            self.loop.run_until_complete(asyncio.gather(*self._on_shutdown))

        tasks = asyncio.Task.all_tasks(self.loop)
        if tasks:
            self.loop.run_until_complete(asyncio.wait(tasks, loop=self.loop, timeout=1))


@pytest.fixture()
def ctx(event_loop):
    context = Context(event_loop)
    if os.getenv('LOG'):
        context.enable_logging(logging.DEBUG)
    yield context
    context.shutdown()


@pytest.fixture
def connection_parameters():
    return {
        'login': os.environ.get('RABBITMQ_DEFAULT_USER', 'guest'),
        'password': os.environ.get('RABBITMQ_DEFAULT_PASS', 'guest'),
        'host': os.environ.get('RABBITMQ_HOST', 'localhost'),
        'port': '5672',
        'virtualhost': os.environ.get('RABBITMQ_DEFAULT_VHOST', '/'),
    }


@pytest.fixture
def exchange_name():
    return os.environ.get('RABBITMQ_EXCHANGE', 'sbus')


@pytest.fixture
def exchange_type():
    return 'direct'


@pytest.fixture
def service_prefix():
    return 'my_test_app'


@pytest.fixture
def queue_name(service_prefix):
    return f'{service_prefix}.queue.test'


@pytest.fixture(scope='session')
def test_topic():
    return 'auth.test'


@pytest.fixture
def another_topic():
    return 'auth.test_double'


@pytest.fixture
def prefetch_count():
    return 1


@pytest.fixture
def worker_name():
    return 'test_worker'


@pytest.fixture
def wrong_worker_name():
    return 'no_test_worker'


@pytest.fixture
def routes(queue_name, test_topic, prefetch_count, worker_name, wrong_worker_name):
    return {
        worker_name: {
            'routing_keys': (test_topic,),
            'queue_name': queue_name,
            'prefetch_count': prefetch_count,
        },
        wrong_worker_name: {
            'routing_keys': (test_topic,),
            'queue_name': queue_name,
            'prefetch_count': prefetch_count,
        }
    }


@pytest.fixture
def subscriber(worker_name, router, routes):
    return QueueSubscriber(router, routes[worker_name])


@pytest.fixture()
def echo_subscriber(test_topic, worker_name, routes):
    _router = Router()

    async def echo(data, **kwargs):
        return data

    _router.register(test_topic)(echo)

    return QueueSubscriber(_router, routes[worker_name])


@pytest.fixture(scope='session')
def fake_route():
    return make_mocked_coro(return_value='test')


@pytest.fixture(scope='session', autouse=True)
def router(test_topic, fake_route):
    router_ = Router()
    router_.register(test_topic)(fake_route)
    return router_


@pytest.fixture
def amqp_client(event_loop, connection_parameters, exchange_name, exchange_type, worker_name):
    client = AMQPTransport(connection_parameters, exchange_name, exchange_type, worker_name,
                           loop=event_loop)
    event_loop.run_until_complete(client.connect())
    event_loop.run_until_complete(client.declare_exchange(exchange_name, durable=False))
    yield client
    event_loop.run_until_complete(client.destroy())
