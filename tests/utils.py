import asyncio
from unittest import mock
from unittest.mock import sentinel


def make_mocked_coro(return_value=sentinel, raise_exception=sentinel):
    """Creates a coroutine mock."""
    @asyncio.coroutine
    def mock_coro(*args, **kwargs):
        if raise_exception is not sentinel:
            raise raise_exception
        return return_value

    return mock.Mock(wraps=mock_coro)
