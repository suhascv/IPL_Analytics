import yaml
file=open('.yaml')
yaml.load(file, Loader=yaml.FullLoader)