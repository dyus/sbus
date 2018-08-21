import typing

from pydantic import BaseModel


class Headers:
    correlation_id = 'correlation-id'

    message_id = 'message-id'

    client_message_id = 'client-message-id'

    retry_attempts_max = 'retry-attempts-max'

    retry_attempt_nr = 'retry-attempt-nr'

    expired_at = 'expired-at'

    timeout = 'timeout'

    routing_key = 'routing-key'

    timestamp = 'timestamp'


class Response(BaseModel):
    status: int = 200
    body: typing.Any = None


class ErrorResponseBody(BaseModel):
    message: str = None
    error: str = None


class Context(BaseModel):
    timeout: int = None
    max_retries: int = None
    attempt_nr: int = None
    routing_key: str = None
    message_id: str = None
    correlation_id: str = None

    class Config:
        allow_extra = True
        fields = {
            'max_retries': {
                'alias': 'retry-attempts-max'
            },
            'attempt_nr': {
                'alias': 'retry-attempt-nr'
            },
            'routing_key': {
                'alias': 'routing-key'
            },
            'message_id': {
                'alias': 'message-id'
            },
            'correlation_id': {
                'alias': 'correlation-id'
            },
        }
