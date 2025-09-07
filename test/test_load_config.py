from src.utils.config_loader import load_config , create_app_config


load =  load_config("src/config/cdc_config.yml")
print(load)