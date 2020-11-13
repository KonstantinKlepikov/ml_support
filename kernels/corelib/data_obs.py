import numpy as np
import pandas as pd
import os
import csv
from core_paths import DATA_PATH, DATA_PATH_TEST
from custex import EmptyExtention


class Processor:

    def __init__(self, path):
        self.path = path


    def checker(self, dict_of_extention):

        ex = None
        for item in dict_of_extention.items():
            if os.path.splitext(self.path)[1] == item[0]:
                ex = item[1]

        return ex


    def viewer(self, s_tree, prefix=''):

        names = os.listdir(self.path)
        s_path = os.path.realpath(self.path)

        for name in names:
            fullpath = os.path.join(s_path, name)
            if os.path.isfile(fullpath):
                s_tree[prefix + name] = fullpath
            elif os.path.isdir(fullpath):
                Processor(self.path + '/' + name).viewer(s_tree, prefix + name + '/')


class Loader:

    def __init__(self, path):
        self.path = path


class CsvLoader(Loader):

    def __init__(self, path, sep, index_col, dtype, parse_dates, encoding):
        Loader.__init__(self, path)
        self.sep = sep
        self.index_col = index_col
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.encoding = encoding

    def dictLoader(self):
        with open(self.path, 'r', encoding=self.encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype, parse_dates=self.parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(self.index_col))
                data_ex= None
            
        return data_ex


class ZipUnpacker:
    pass


def sourcer(path):

    """Look for source tree and make dict, where keys are file or directory name,
    values are path to file or directory
    """

    s_tree = {}
    Processor(path).viewer(s_tree)

    print('File .... fell path:')
    for key, val in s_tree.items():
        print('{0} ..... {1}'.format(key, val))


def loader(slack, path=DATA_PATH, sep=',', index_col=None, dtype=None, parse_dates=False, encoding=None):

    """Read files and return pandas dataframe

    sep:
        Delimiter to use
        string: default ','

    index_col:
        Column to use as the row labels of the DataFrame, either given as string name or column index.
        If a sequence of int/str is given, a MultiIndex is used.
        Note: index_col=False can be used to force pandas to not use the first column as the index, e.g. when 
        you have a malformed file with delimiters at the end of each line.
        int, str, sequence of int/str, or False: default None

    dtype: 
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
        Use str or object together with suitable na_values settings to preserve and not interpret dtype.
        If converters are specified, they will be applied INSTEAD of dtype conversion
        As example looks like 'dtype': {'assigned_day': np.float64}
        dict, default None

    parse_dates:
        Parse column as datetime format
        If True -> try parsing the index.
        List of int or names. e.g. If [1, 2, 3] -> try parsing columns 1, 2, 3 each as a separate date column.
        List of lists. e.g. If [[1, 3]] -> combine columns 1 and 3 and parse as a single date column.
        Dict, e.g. {‘foo’ : [1, 3]} -> parse columns 1, 3 as date and call result ‘foo’
        More information [pandas.read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)
        If a column or index cannot be represented as an array of datetimes, 
        say because of an unparseable value or a mixture of timezones, the column 
        or index will be returned unaltered as an object data type.
        bool or list of int or names or list of lists or dict: default False

    encoding: 
        Encoding to - use for UTF when reading/writing (ex. ‘utf-8’)
        str: default None

    Raises:
        EmptyExtention: raised, if no one extention is available in list of readable file extentions

    Returns:
        obj: Pandas dataFrame object
    """

    path = os.path.realpath(path + '/' + slack)
    dict_of_extention = {'.csv': CsvLoader(path, sep, index_col, dtype, parse_dates, encoding)}
    process = Processor(path).checker(dict_of_extention)

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
    process = Processor(path).checker(dict_of_extention)


if __name__ == "__main__":
    
    sourcer(path=DATA_PATH_TEST)
    print('sourcer.... ok')

    loaded = loader('titanic.csv', path=DATA_PATH_TEST)
    if loaded:
        print(loaded)
        print('loader .... ok')
