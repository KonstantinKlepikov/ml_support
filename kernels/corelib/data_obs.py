import numpy as np
import pandas as pd
import os
import csv
from core_paths import DATA_PATH, DATA_PATH_TEST
from custex import EmptyExtention


class Processor:
    pass


class ZipUnpacker(Processor):
    pass


class CsvLoader(Processor):
    pass


def checker(path, dict_of_extention):

    ex = None
    for item in dict_of_extention.items():
        if os.path.splitext(path)[1] == item[0]:
            ex = item[1]

    return ex


def sourcer(path):

    """Look for source tree and make dict, where keys are file or directory name,
    values are path to file or directory
    """

    s_tree = {}
    prefix = ''

    def viewer(path, prefix, s_tree):

        names = os.listdir(path)
        s_path = os.path.realpath(path)

        for name in names:
            fullpath = os.path.join(s_path, name)
            if os.path.isfile(fullpath):
                s_tree[prefix + name] = fullpath
            elif os.path.isdir(fullpath):
                viewer(path + '/' + name, prefix + name + '/', s_tree)

    viewer(path, prefix, s_tree)

    print('File .... fell path:')
    for key, val in s_tree.items():
        print('{0} ..... {1}'.format(key, val))


def loader(slack, path=DATA_PATH):

    """Read files and return pandas dataframe

    Raises:
        EmptyExtention: raised, if no one extention is available in list of readable file extentions

    Returns:
        obj: Pandas dataFrame object
    """

    path = os.path.realpath(path + '/' + slack)
    dict_of_extention = {'.csv': CsvLoader()}
    process = checker(path=path, dict_of_extention=dict_of_extention)

    if process:
        return process
    else:
        try:
            raise EmptyExtention
        except EmptyExtention:
            print('Extention of file is wrong! Choose another file')


def unpacker(slack, path=DATA_PATH):

    path = os.path.realpath(path + slack)
    dict_of_extention = {'.zip': ZipUnpacker()}
    process = checker(path=path, dict_of_extention=dict_of_extention)


if __name__ == "__main__":
    
    sourcer(path=DATA_PATH_TEST)
    print('sourcer.... ok')

    if loader('titanic.csv', path=DATA_PATH_TEST):
        print('loader .... ok')
