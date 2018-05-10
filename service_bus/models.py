import typing

from pydantic import BaseModel, Required


class Headers:
    correlation_id = 'correlation-id'

    message_id = 'message-id'

    client_message_id = 'client-message-id'

    retry_attempts_max = 'retry-attempts-max'

    retry_attempt_nr = 'retry-attempts-nr'

    expired_at = 'expired-at'

    timeout = 'timeout'

    routing_key = 'routing-key'


class Response(BaseModel):
    status: int = 200
    body: typing.Any = None


class ResponseBody(BaseModel):
    error: str = None
    message: str = None


class Context(BaseModel):
    routing_key: str = Required

    message_id: str = None
