import json
from RetailerBase import RetailerBase
from typing import Dict, Any, List
from Utility import log_message
import importlib

class RetailerFactory:
    def __init__(self, config_file: str):
        log_message(f"init RetailerFactory")
        self.config = self._load_config(config_file)
        self.retailers = self._initialize_retailers()

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        log_message(f"RetailerFactory.load config file called")
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in configuration file: {config_file}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

    def _initialize_retailers(self) -> Dict[str, RetailerBase]:
        log_message(f"RetailerFactory._initialize_retailers called")
        retailers = {}
        #log_message(f'{list(self.config.items())}')
        
        for retailer_name, strategies in self.config.items():
            retailer_class_name = f"{retailer_name.capitalize()}Retailer"
            try:
                module_name = f"RetailerClasses.{retailer_class_name}"
                module = importlib.import_module(module_name)
                retailer_class = getattr(module, retailer_class_name)
                #retailer_class = globals().get(retailer_class_name)
                # if retailer_class is None:
                #     print(f"Warning: Retailer class {retailer_class_name} not found. Skipping.")
                #     continue
                log_message(f'{retailer_class}')
                retailer_config = self._parse_retailer_strategies(strategies)
                retailers[retailer_name] = retailer_class(retailer_config)
            except (ModuleNotFoundError, AttributeError) as e:
                #log_message(f"Warning: Retailer class {retailer_class_name} not found. Skipping.")
                pass
        log_message(f"RetailerFactory._initialize_retailers Ended: Size of retailers:{len(retailers)}")
        return retailers

    def _parse_retailer_strategies(self, strategies: Dict[str, Any]) -> Dict[str, Any]:
        log_message(f"RetailerFactory._parse_retailer_strategies called")
        parsed_strategies = {}
        for strategy_name, strategy_config in strategies.items():
            parsed_strategy = {
                "class": strategy_config.get("class"),
                "URLidentifier": strategy_config.get("URLidentifier"),
                "Valueidentifier": strategy_config.get("Valueidentifier", []),
                "Method": strategy_config.get("Method")
            }
            parsed_strategies[strategy_name] = parsed_strategy
        log_message(f"RetailerFactory._parse_retailer_strategies end:{parsed_strategies}")
        return parsed_strategies

    def get_retailer(self, retailer_name: str) -> RetailerBase:
        log_message(f"RetailerFactory.get_retailer called")
        retailer = self.retailers.get(retailer_name)
        if retailer is None:
            raise ValueError(f"Retailer not found: {retailer_name}")
        log_message(f"RetailerFactory.get_retailer end")
        return retailer