"""
Exceptions, related to messaging.
"""


class UnrecoverableExceptionBase(Exception):
    """Exception for negative ack to service bus"""


class StreamConnectionError(Exception):
    """Error connecting transport"""


class ConsumerError(StreamConnectionError):
    """Some error in message consumer"""


class PublisherError(StreamConnectionError):
    """Problem with message publisher"""


class SerializationError(UnrecoverableExceptionBase):
    """Error serializing or deserializing data"""


class StreamInterrupt(BaseException):
    """Error for stop StreamWorker loop gracefully."""


class RecoverableErrorBase(Exception):
    pass


class TooManyRequestError(RecoverableErrorBase):
    status = 429


class BadRequestError(UnrecoverableExceptionBase):
    status = 400


class UnauthorizedError(UnrecoverableExceptionBase):
    status = 401


class ForbiddenError(UnrecoverableExceptionBase):
    status = 403


class NotFoundError(UnrecoverableExceptionBase):
    status = 404


class MethodNotAllowedError(UnrecoverableExceptionBase):
    status = 405


class UnrecoverableError(UnrecoverableExceptionBase):
    status = 456


class ServiceUnavailableError(RecoverableErrorBase):
    status = 503


class InternalServerError(RecoverableErrorBase):
    status = 500


from_code_exception = {
    429: TooManyRequestError,
    400: BadRequestError,
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    405: MethodNotAllowedError,
    456: UnrecoverableError,
    503: ServiceUnavailableError,
    500: InternalServerError
}
