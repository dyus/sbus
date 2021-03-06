import inspect
import logging
import typing
import uuid
from abc import ABCMeta, abstractmethod

from sbus.models import Response

from .router import Router

logger = logging.getLogger(__name__)


class AbstractSubscriber(metaclass=ABCMeta):
    """Handle and process queue messages."""

    def __init__(self, routing: Router, subscriber_config: typing.Dict,
                 **handler_kwargs):
        assert subscriber_config, 'Subscriber configuration should be provided'
        self.config = subscriber_config
        self.routing = routing
        self.handler_kwargs = handler_kwargs

    @abstractmethod
    async def on_message(self, data, routing_key, context, serializer):
        pass

    @property
    @abstractmethod
    def consumer_tag(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def routing_keys(self):
        pass

    @property
    @abstractmethod
    def durable(self):
        pass

    @property
    @abstractmethod
    def prefetch_count(self):
        pass

    def __str__(self):
        return f'<Subscriber: name={self.name} durable={self.durable}>'

    def __repr__(self):
        return self.__str__()


class QueueSubscriber(AbstractSubscriber):

    def consumer_tag(self):
        return 'consumer_tag.{}.{}'.format(self.name, uuid.uuid4().hex)

    @property
    def name(self) -> str:
        return self.config.get('queue_name', 'undefined')

    @property
    def durable(self) -> str:
        return self.config.get('durable', True)

    @property
    def prefetch_count(self) -> str:
        return self.config.get('prefetch_count', 0)

    @property
    def routing_keys(self):
        return self.routing.routing.keys()

    async def on_message(self, data: bytes, routing_key: typing.AnyStr, context: typing.Dict, serializer):  # noqa
        handler = self.routing.get_handler(routing_key)
        model = self._get_request_model(handler)
        request_body = serializer.deserialize(data).body
        response = await handler(model(**request_body), context, **self.handler_kwargs)
        return serializer.serialize(Response(body=response))

    @staticmethod
    def _get_request_model(handler):
        return inspect.signature(handler).parameters['task'].annotation
