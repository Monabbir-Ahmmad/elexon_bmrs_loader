from collections import defaultdict
from interfaces.subscriber import Subscriber


class EventManager:
    def __init__(self):
        self.subscribers = defaultdict(list)

    def subscribe(self, event: str, listener: Subscriber):
        self.subscribers[event].append(listener)

    def unsubscribe(self, event: str, listener: Subscriber):
        if event in self.subscribers:
            self.subscribers[event].remove(listener)

    def notify(self, event: str, data):
        for listener in self.subscribers.get(event, []):
            listener.update(data)
