import os
from core_paths import DATA_PATH


class Processor:
    pass


class ZipUnpacker(Processor):
    pass


class CsvLoader(Processor):
    pass


def checker(path, dict_of_extention):
    return dict_of_extention.values()


def sourcer(path=DATA_PATH):

    """
    Look for source tree and make dict, where keys are file or directory name,
    values are path to file or directory
    """

    names = os.listdir(path)
    path = os.path.realpath(path)
    s_tree = {}

    for name in names:
        fullname = os.path.join(path, name)
        if os.path.isfile(fullname):
            s_tree[name] = fullname
        elif os.path.isdir(fullname):
            if sourcer(fullname):
                s_tree.update(sourcer(fullname))

    for key, val in s_tree.items():
        print('{0} ..... {1}'.format(key, val))


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
    
    sourcer()

