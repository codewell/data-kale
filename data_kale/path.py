from pathlib import Path

from data_kale.configuration import configuration


def data_root():
    return Path(configuration()['data']['root']).expanduser()

def repository_path(repository_name):
    return data_root() / repository_name