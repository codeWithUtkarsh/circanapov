from abc import ABC, abstractmethod
from typing import Dict, Any, List
from URLStrategy import URLStrategy, findBySKUURL, findByDescURL, findBySKUDescURL, findByModelNumURL
from Utility import log_message
import importlib

class RetailerBase(ABC):
    def __init__(self, config: Dict[str, Any]):
        log_message(f"init RetailerBase")
        self.config = config
        self.strategies = self._initialize_strategies()

    def _initialize_strategies(self) -> Dict[str, URLStrategy]:
        log_message(f"RetailerBase._initialize_strategies called")
        strategies = {}
        for strategy_name, strategy_config in self.config.items():
            method_name = strategy_config['Method']
            try:
                module = importlib.import_module("URLStrategy")
                strategy_class = getattr(module, method_name) 
                strategies[strategy_name] = strategy_class(strategy_config['URLidentifier'])
            except (ModuleNotFoundError, AttributeError) as e:
                log_message(f"Error: Strategy class {method_name} not found in URLStrategy. Skipping strategy {strategy_name}.")
        return strategies

    def get_url(self, strategy_name: str, **kwargs) -> str:
        log_message(f"RetailerBase.get_url called")
        strategy = self.strategies.get(strategy_name)
        if not strategy:
            raise ValueError(f"Strategy not found: {strategy_name}")
        return strategy.generate_url(**kwargs)

    @abstractmethod
    def process_json(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        log_message(f"RetailerBase.process_json called")
        pass

    @abstractmethod
    def process_html(self, html_data: str) -> Dict[str, Any]:
        log_message(f"RetailerBase.process_html called")
        pass

    @abstractmethod
    def merge_data(self, json_data: Dict[str, Any], html_data: Dict[str, Any]) -> Dict[str, Any]:
        log_message(f"RetailerBase.merge_data called")
        pass
