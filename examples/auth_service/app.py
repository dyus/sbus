import asyncio
import logging

from pydantic import BaseModel

from sbus.models import Context
from sbus.router import Router
from sbus.sbus import Sbus
from sbus.subscribers import QueueSubscriber

logger = logging.getLogger('sbus.transports.aiopika')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

CONFIG = {
    'transport': {
        'transports': {
            'aiopika': {
                'connection_parameters': {
                    'login': 'guest',
                    'password': 'guest',
                    'host': '0.0.0.0',
                    'port': 5672,
                    'virtualhost': '/',
                },
                'exchange_name': 'test',
                'exchange_type': 'direct',
            },
        },
        'active_transport': 'aiopika',
    },

    'subscriber': {
        'queue_name': 'auth',
        'prefetch_count': 1,
        'durable': False
    }
}

routing = Router()


class AuthModel(BaseModel):
    login: str
    password: str


class User(BaseModel):
    user_id: str
    token: str


@routing.register('auth')
async def auth_handler(task: AuthModel, context: Context, sbus: Sbus):
    result = await sbus.request(task, 'auth.bearer', context, User)
    return result


@routing.register('auth.bearer')
async def check_bearer_login(task: AuthModel, context: Context, sbus: Sbus):
    return User(user_id=task.login, token=task.login + task.password)


def main():
    loop = asyncio.get_event_loop()

    sbus = Sbus.create_factory(CONFIG['transport'], loop)()
    loop.run_until_complete(sbus.connect())
    loop.run_until_complete(
        sbus.on(QueueSubscriber(routing, subscriber_config=CONFIG['subscriber'], sbus=sbus))
    )
    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        loop.run_until_complete(sbus.transport.destroy())


if __name__ == '__main__':
    main()
