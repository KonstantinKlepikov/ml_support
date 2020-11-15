import numpy as np
import pandas as pd
import os
import csv
from zipfile import ZipFile
from zipfile import BadZipfile
from core_paths import DATA_PATH, DATA_PATH_TEST, DATA_OUTPUT_TEST
from custex import EmptyExtention


class Processor:

    """Class provides methods for display and check data source
    """

    def __init__(self, path):
        self.path = path

    def checker(self, dict_of_extention):

        """Check data sorce

        Returns:
            obj: class exemplar for data processing for spurce file
        """

        ex = None
        for item in dict_of_extention.items():
            if os.path.splitext(self.path)[1] == item[0]:
                ex = item[1]

        return ex

    def viewer(self, s_tree, prefix=''):

        """Display source tree
        """

        names = os.listdir(self.path)
        s_path = os.path.realpath(self.path)

        for name in names:
            fullpath = os.path.join(s_path, name)
            if os.path.isfile(fullpath):
                s_tree[prefix + name] = fullpath
            elif os.path.isdir(fullpath):
                Processor(self.path + '/' + name).viewer(s_tree, prefix + name + '/')


class Loader:

    """Base class to data load
    """

    def __init__(self, path, index_col, dtype, parse_dates):
        self.path = path
        self.index_col = index_col
        self.dtype = dtype
        self.parse_dates = parse_dates


class CsvLoader(Loader):

    """Load .csv files
    """

    def __init__(self, path, index_col, dtype, parse_dates, sep, encoding):
        Loader.__init__(self, path, index_col, dtype, parse_dates)
        self.sep = sep
        self.encoding = encoding

    def pd_load(self):

        """Loads csv to pd

        Returns:
            obj: pandas DataFrame
        """

        with open(self.path, 'r', encoding=self.encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype, parse_dates=self.parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(self.index_col))
                data_ex = None
            
        return data_ex


class Unpacker:

    """Base class to unpack load
    """

    def __init__(self, path, output):
        self.path = path
        self.output = output


class ZipUnpacker(Unpacker):

    """Unpack .zip files
    """

    def __init__(self, path, output):
        Unpacker.__init__(self, path, output)
    
    def unpack(self):

        """Unpack .zip files
        """

        with ZipFile(self.path, 'r') as g:
            inside = g.infolist()
            for ig in inside:
                try:
                    g.extract(ig, self.output)
                    print('{} ... unpacked'.format(ig))
                except BadZipfile:
                    print('{} ... wrong file'.format(ig))


def sourcer(path):

    """Look for source tree and make dict, where keys are file or directory name,
    values are path to file or directory
    """

    s_tree = {}
    Processor(path).viewer(s_tree)

    print('File .... fell path:')
    for key, val in s_tree.items():
        print('{0} ..... {1}'.format(key, val))


def loader(slack, path=DATA_PATH, index_col=None, dtype=None, parse_dates=False, sep=',', encoding=None):

    """Read files and return pandas dataframe

    slack:
        Part of file path/
        string: looks like this/that.csv

    path:
        Base placement of data files
        string: default DATA_PATH

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

    sep:
        Delimiter to use
        string: default ','

    encoding: 
        Encoding to - use for UTF when reading/writing (ex. ‘utf-8’)
        str: default None

    Raises:
        EmptyExtention: raised, if no one extention is available in list of readable file extentions

    Returns:
        obj: Pandas dataFrame object
    """

    path = os.path.realpath(path + '/' + slack)
    dict_of_extention = {'.csv': CsvLoader(path, index_col, dtype, parse_dates, sep, encoding)}
    process = Processor(path).checker(dict_of_extention)

    if process:
        return process
    else:
        try:
            raise EmptyExtention
        except EmptyExtention:
            print('Extention of file is wrong! Choose another file')


def unpacker(slack, path=DATA_PATH, output=''):

    output = os.path.realpath(path + output)
    path = os.path.realpath(path + '/' + slack)
    dict_of_extention = {'.zip': ZipUnpacker(path, output)}
    process = Processor(path).checker(dict_of_extention)
    process.unpack()


if __name__ == "__main__":
    
    sourcer(path=DATA_PATH_TEST)
    print('sourcer.... ok')

    loaded = loader('titanic.csv', path=DATA_PATH_TEST).pd_load()
    print(loaded.head(1))
    print('loader .... ok')

    unpacker('titanic.zip', path=DATA_PATH_TEST, output=DATA_OUTPUT_TEST)
    print('unpacker .... ok')
