
class Router:
    """Register your consumers here."""
    _routing = {}

    def __init__(self, subscribers=None):
        if subscribers is not None:
            self.routing.update(subscribers)

    @property
    def routing(self):
        return self._routing

    def register(self, *routing_keys):
        def deco(func):
            for key in routing_keys:
                self.routing.update({key: func})
            return func

        return deco

    def get_handler(self, routing_key):
        return self.routing.get(routing_key)
