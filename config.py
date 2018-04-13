import yaml
import yaml.scanner


config = dict()
config_auth = dict()

try:
    with open("config.yml", "r") as f:
        try:
            config = yaml.safe_load(f) or {}
        except yaml.scanner.ScannerError as e:
            raise Exception("Configuration file at 'config.yml' contains invalid YAML...")
        except Exception as e:
            print(type(e))
except FileNotFoundError as e:
    raise Exception("Configuration file not found at: 'config.yml'...")

try:
    with open("config.auth.yml", "r") as f:
        try:
            config_auth = yaml.safe_load(f) or {}
        except yaml.scanner.ScannerError as e:
            raise Exception(f"Configuration file at 'config.auth.yml' contains invalid YAML...")
        except Exception as e:
            print(type(e))
except FileNotFoundError as e:
    raise Exception(f"Configuration file not found at: 'config.auth.yml'...")

config = {**config_auth, **config}
