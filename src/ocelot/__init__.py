import yaml
from pkg_resources import resource_filename

with open(resource_filename(__name__, 'config.yaml'), 'r') as y:
    config = yaml.safe_load(y)
