import json

CONFIG_FILE = 'config.json'
DEFAULTS = {
    'default_max_retries': 3,
    'base_backoff_seconds': 2
}

def load_config():
    """Loads configuration from config.json"""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        save_config(DEFAULTS)
        return DEFAULTS
    except json.JSONDecodeError:
        print(f"Warning: {CONFIG_FILE} is corrupted. Using defaults.")
        return DEFAULTS

def save_config(config_data):
    """Saves configuration to config.json"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception as e:
        print(f"Error saving config: {e}")
        return False