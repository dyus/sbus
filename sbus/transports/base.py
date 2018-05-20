"""
Abstract and bases for building messaging stuff.
"""

from abc import ABCMeta, abstractmethod


class AbstractTransport(metaclass=ABCMeta):

    @abstractmethod
    async def connect(self):
        pass

    @property
    @abstractmethod
    def connected(self):
        pass

    @property
    @abstractmethod
    def transport(self):
        pass

    @property
    @abstractmethod
    def protocol(self):
        pass

    @abstractmethod
    async def command(self, data, routing_key):
        pass

    @abstractmethod
    async def on(self, subscriber):
        pass

    @abstractmethod
    async def request(self, data, routing_key, response_class):
        pass

    @abstractmethod
    async def destroy(self):
        pass
