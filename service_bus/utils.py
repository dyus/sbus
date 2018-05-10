from .transports import SUPPORTED_TRANSPORTS


def create_transport(config, loop):
    """Create transport from configured settings.

        Will definitely fail if there is no meaningful configuration provided.
        Example of such is:
            {
                'transports': {
                    'ampq': {
                        'connection_parameters': {
                            'login': 'guest',
                            'password': '',
                            'host': 'localhost',
                            'port': 5672,
                            'virtualhost': 'sbus',
                        },
                        'exchange_name': 'sbus',
                        'exchange_type': 'topic',
                    },
                },
                'active_transport': 'amqp',
            }
        """
    active_transport = config['active_transport']

    assert active_transport in SUPPORTED_TRANSPORTS, f'Unsupported transport "{active_transport}"'

    active_transport_setup = config['transports'][active_transport]

    transport_class = SUPPORTED_TRANSPORTS[active_transport]
    transport = transport_class(**active_transport_setup, loop=loop)

    return transport
