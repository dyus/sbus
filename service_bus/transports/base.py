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
    async def publish(self, data, routing_key):
        pass

    @abstractmethod
    async def subscribe(self, subscriber):
        pass

    @abstractmethod
    async def request(self, data, topic, callback=None):
        pass

    @abstractmethod
    async def destroy(self):
        pass
