import os
from .core_paths import DATA_PATH


class Processor:
    pass


class ZipUnpacker(Processor):
    pass


class CsvLoader(Processor):
    pass


def checker(path, dict_of_extention):
    return dict_of_extention.values()


def sourcer(path=DATA_PATH):
    path = os.path.realpath(path)


def loader(path=DATA_PATH, slack=''):
    path = os.path.realpath(path + slack)
    dict_of_extention = {'.csv': CsvLoader()}
    process = checker(path=path, dict_of_extention=dict_of_extention)
    return process


def unpacker(path=DATA_PATH, slack=''):
    path = os.path.realpath(path + slack)
    dict_of_extention = {'.zip': ZipUnpacker()}
    process = checker(path=path, dict_of_extention=dict_of_extention)


if __name__ == "__main__":
    pass
