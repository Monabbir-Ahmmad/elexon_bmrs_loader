from abc import ABC, abstractmethod

class Subscriber(ABC):
    @abstractmethod
    def update(self, data) -> None:
        pass