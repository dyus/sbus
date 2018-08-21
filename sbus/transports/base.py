"""
Abstract and bases for building messaging stuff.
"""

from abc import ABCMeta, abstractmethod


class AbstractTransport(metaclass=ABCMeta):

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def send(self, data, routing_key, context, response_class=None):
        pass

    @abstractmethod
    async def destroy(self):
        pass
