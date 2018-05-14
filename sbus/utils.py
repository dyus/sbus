from .transports import SUPPORTED_TRANSPORTS


def my_import(name):
    components = name.split('.')
    mod = __import__('.'.join(components[:-1]))
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


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
    if 'serializer' in active_transport_setup:
        active_transport_setup['serializer'] = my_import(active_transport_setup.get('serializer'))()
    transport = transport_class(**active_transport_setup, loop=loop)

    return transport
