from abc import ABC, abstractmethod
from Utility import log_message

class URLStrategy(ABC):
    def __init__(self, url_identifier: str):
        log_message(f"init URLStrategy called")
        self.url_identifier = url_identifier

    @abstractmethod
    def generate_url(self, **kwargs) -> str:
        log_message(f"URLStrategy.generate_url called")
        pass

class findBySKUURL(URLStrategy):
    def generate_url(self, **kwargs) -> str:
        log_message(f"URLStrategy.findBySKUURL called")
        return f"{self.url_identifier}{kwargs['SKU']}"

class findByDescURL(URLStrategy):
    def generate_url(self, **kwargs) -> str:
        log_message(f"URLStrategy.findByDescURL called")
        return f"{self.url_identifier}{kwargs['Description']}"

class findBySKUDescURL(URLStrategy):
    def generate_url(self, **kwargs) -> str:
        log_message(f"URLStrategy.findBySKUDescURL called")
        return f"{self.url_identifier}{kwargs['UPC']}/{kwargs['Description']}"

class findByModelNumURL(URLStrategy):
    def generate_url(self, **kwargs) -> str:
        log_message(f"URLStrategy.findByModelNumURL called")
        return f"{self.url_identifier}{kwargs['ModelNum']}"