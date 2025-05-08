import logging
from configparser import ConfigParser 
from logging.handlers import RotatingFileHandler
import pathlib

logger = logging.getLogger(__name__)
DWH_DATA_REFRESH = 'dwh-data-refresh'


def get_config_data_refresh():
    return get_config(filename='dwh_data_refresh.ini')


def get_config(filename):
    parser = ConfigParser()
    
    # This resolves the path relative to utils.py
    config_path = pathlib.Path(__file__).parent / filename

    if not config_path.exists():
        logger.error(f"Config file not found at: {config_path}")
        return {}

    parser.read(config_path)

    config_dict = {}

    for element in parser.sections():
        config_dict[element] = {}
        for name, value in parser.items(element):
            config_dict[element][name] = value

    return config_dict

def get_log_level(level):
    if level:
        level = str(level).upper()

    switcher = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    # default
    return switcher.get(level, logging.INFO)

def get_log():
    logger = logging.getLogger(DWH_DATA_REFRESH)
    return logger

def init_log(c):
    # pass in config to allow separate environments
    # c = get_config()
    # print(c)
    ensure_path_exists(c['log']['log_file'])
    # logger name can be generic, log filename is in configuration file
    logger = logging.getLogger(DWH_DATA_REFRESH)
    log_level = get_log_level(c['log']['log_level'])
    logging.basicConfig(level=log_level,
                        format="%(asctime)s:%(levelname)s:%(filename)s:%(message)s",
                        handlers=[
                            logging.FileHandler("{0}".format(c['log']['log_file'])),
                            logging.StreamHandler()
                        ])
    # add a rotating handler
    handler = RotatingFileHandler(c['log']['log_file'], maxBytes=10*1024*1024,
                                  backupCount=5)
    logger.addHandler(handler)
    return logger

def ensure_path_exists(path):
    pathlib.Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)

def batch(iterable, n):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)] 

def update_execution_status_logs(file_path, message):
     with open(file_path, 'a+', encoding="utf-8") as f:
        f.write(message + "\n")
