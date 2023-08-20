import datetime
import yaml

def load_config():
    with open('config/config.yaml', 'r') as config_file:
        return yaml.safe_load(config_file)

def validate_input_date(input_date):
    try:
        datetime.datetime.strptime(input_date, '%Y/%m/%d')
        return True
    except ValueError:
        return False
