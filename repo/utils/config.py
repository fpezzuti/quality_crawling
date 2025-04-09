# config.py
import yaml

CONFIG_FPATH = "./configs/config.yaml"

class Config:
    def __init__(self, config_path: str = CONFIG_FPATH):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def get(self, section, option = None, default=None):
        sec = self.config.get(section, {})  
        return sec if option is None else sec.get(option, default)

# ensure config is loaded once
config = Config()