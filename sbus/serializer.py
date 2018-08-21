import enum
import json
import logging
from datetime import date, datetime, timedelta

from pydantic import ValidationError

from sbus.models import Response

from .exceptions import BadRequestError, SerializationError

logger = logging.getLogger(__name__)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()

    elif issubclass(obj.__class__, enum.Enum):
        serial = obj.value

    elif isinstance(obj, timedelta):
        serial = str(obj)

    else:
        raise SerializationError(f'Type not serializable {type(obj)} in {obj}')

    return serial


class JSONSerializer:
    def serialize(self, data: Response) -> bytes:
        try:
            serialized = json.dumps(
                data.dict(),
                ensure_ascii=False,
                default=json_serial).encode()
        except Exception as error:
            logger.exception('Can\'t serialization message: %s. Because of %s', data, str(error))
            raise SerializationError from error
        return serialized

    def deserialize(self, msg) -> Response:
        body_txt = msg.decode('utf-8') if hasattr(msg, 'decode') else msg
        try:
            deserialized = json.loads(body_txt)
        except json.JSONDecodeError as error:
            logger.exception('Can\'t deserialize message: %s. Because of %s',
                             body_txt, str(error))
            raise SerializationError from error

        try:
            return Response(**deserialized)
        except ValidationError as error:
            logger.exception('Invalid message: %s. Because of %s', body_txt, str(error))
            raise BadRequestError(str(error)) from error
