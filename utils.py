from configparser import ConfigParser 


def get_config_nf():
    return get_config(filename='properties.ini')

def get_config(filename):
    parser = ConfigParser()
    parser.read(filename)

    config_dict = {}

    for element in parser.sections():
        config_dict[element] = {}
        for name, value in parser.items(element):
            config_dict[element][name] = value

    return config_dict

