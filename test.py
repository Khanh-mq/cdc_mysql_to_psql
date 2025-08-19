import yaml
#open config file 
with open('config/cdc_config.yml', 'r') as file:
    config = yaml.safe_load(file)
print(config)