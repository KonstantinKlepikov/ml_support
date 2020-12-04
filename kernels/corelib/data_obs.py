import numpy as np
import pandas as pd
import os, csv, shelve
from zipfile import ZipFile
from zipfile import BadZipfile
from custex import EmptyProcess
import core_paths

class Processor:

    def __init__(self, path_=core_paths.DATA_PATH, inslack='', outslack=''):
        self.path_ = path_
        self.inslack = inslack
        self.outslack = outslack
        self.in_path = os.path.realpath(os.path.join(path_, inslack))
        self.out_path = os.path.realpath(os.path.join(path_, outslack))

    @staticmethod
    def _rjpath(path_, slack):

        return os.path.realpath(os.path.join(path_, slack))

    @staticmethod
    def _sourcer(path_, inslack, _s_tree, _prefix=''):

        """Look for source tree and make dict, where keys are file or directory name,
        values are path to file or directory

        path_:
            Base placement of data files
            string: default DATA_PATH
        """

        names = os.listdir(path_)
        s_path = os.path.realpath(path_)

        for name in names:
            fullpath = os.path.join(s_path, name)
            if os.path.isfile(fullpath):
                _s_tree[_prefix + name] = fullpath
            elif os.path.isdir(fullpath):
                Processor._sourcer(os.path.join(path_, name), inslack, _s_tree, _prefix + name + '/')

        return _s_tree

    @staticmethod
    def checker(in_path, dict_of_extention):

        """Check data sorce

        Returns:
            obj: class exemplar for data processing for spurce file
        """

        ex = None
        for item in dict_of_extention.items():
            if os.path.splitext(in_path)[1] == item[0]:
                ex = item[1]

        return ex


    def source(self):

        """Print source tree
        """

        _s_tree = {}
        _s_tree = self._sourcer(self.path_, self.inslack, _s_tree)

        print('File .... fell path:')
        for key, val in _s_tree.items():
            print('{0} ..... {1}'.format(key, val))


class Loader(Processor):

    """Base class to data load
    """

    def __init__(self, path_=core_paths.DATA_PATH, inslack='', outslack='', index_col=None, dtype=None, parse_dates=False, sep=',', encoding=None):
        super().__init__(path_, inslack, outslack)
        self.index_col = index_col
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.sep = sep
        self.encoding = encoding

    @staticmethod
    def _csv_load(in_path, index_col, dtype, parse_dates, sep, encoding):

        """Loads csv to pd

        Returns:
            obj: pandas DataFrame
        """

        with open(in_path, 'r', encoding=encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=sep, index_col=index_col, dtype=dtype, parse_dates=parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(index_col))
                data_ex = None
            
        return data_ex

    @staticmethod
    def check_the_output(process):

        if process is True:
            return process
        else:
            try:
                raise EmptyProcess
            except EmptyProcess:
                print('Extention of file is wrong! Choose another file')

    
    def load(self):

        dict_of_extention = {'.csv': self._csv_load(self.in_path, self.index_col, self.dtype, self.parse_dates, self.sep, self.encoding)}
        process = self.checker(self.in_path, dict_of_extention)

        return self.check_the_output(process)


    def view(self):
        pass


class CSVLoader(Loader):


        def _csv_load(in_path, index_col, dtype, parse_dates, sep, encoding):

        """Loads csv to pd

        Returns:
            obj: pandas DataFrame
        """

        with open(in_path, 'r', encoding=encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=sep, index_col=index_col, dtype=dtype, parse_dates=parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(index_col))
                data_ex = None
            
        return data_ex


cla


class CsvLoader(Loader):

    """Load .csv files
    """

    def __init__(self, path_, inslack, index_col, dtype, parse_dates, sep, encoding):
        super().__init__(path_, inslack, index_col, dtype, parse_dates)
        self.sep = sep
        self.encoding = encoding

    def load(self):

        """Loads csv to pd

        Returns:
            obj: pandas DataFrame
        """

        with open(self.path_, 'r', encoding=self.encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype, parse_dates=self.parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(self.index_col))
                data_ex = None
            
        return data_ex

    def save(self):
        pass


class Unpacker(Processor):

    """Base class to unpack load
    """

    def __init__(self, path_, inslack, outslack):
        self.path_ = self._rjpath(path_, inslack)
        self.outslack = self._rjpath(path_, outslack)


class ZipUnpacker(Unpacker):

    """Unpack .zip files
    """

    def __init__(self, path_, inslack, outslack):
        super().__init__(path_, inslack, outslack)
    
    def unpack(self):

        """Unpack .zip files
        """

        with ZipFile(self.path_, 'r') as g:
            inside = g.infolist()
            for ig in inside:
                try:
                    g.extract(ig, self.outslack)
                    print('{} ... unpacked'.format(ig))
                except BadZipfile:
                    print('{} ... wrong file'.format(ig))

    def pack(self):
        pass


class DataDump(Processor):

    """Class provide method constructor for serialise data
    """

    def __init__(self, path_, inslack):
        super().__init__(path_, inslack)


class ShelveDump(DataDump):

    """Class provide methods for save and load data with shelve
    """

    def dump(self, dump_list):
        print(self.path_)
        with shelve.open(self.path_) as s:
            for k, v in enumerate(dump_list):
                try:
                    s[str(k)] = v
                    print('Object {0} is dumped to "{1}" objects'.format(k, self.path_))
                except TypeError:
                    print('Object {0} not dumped - an error occurred'.format(k))

    def undump(self):
        dict_of_objects = {}
        with shelve.open(self.path_) as o:
            for k, v in o.items():
                dict_of_objects[k] = v
            return dict_of_objects.values()


def checker(inslack, path_, dict_of_extention):

    """Check data sorce

    Returns:
        obj: class exemplar for data processing for spurce file
    """

    path_ = os.path.realpath(os.path.join(path_, inslack))
    ex = None
    for item in dict_of_extention.items():
        if os.path.splitext(path_)[1] == item[0]:
            ex = item[1]

    return ex

def check_the_output(process):

    if process:
        return process
    else:
        try:
            raise EmptyProcess
        except EmptyProcess:
            print('Extention of file is wrong! Choose another file')


def loader(inslack, path_=core_paths.DATA_PATH, index_col=None, dtype=None, parse_dates=False, sep=',', encoding=None):

    """Read file and return pandas dataframe

    inslack:
        Part of file path
        string: looks like this/that.csv

    path_:
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

    dict_of_extention = {'.csv': CsvLoader(inslack, path_, index_col, dtype, parse_dates, sep, encoding)}
    process = checker(inslack, path_, dict_of_extention)

    return check_the_output(process).pd_load()


def unpacker(inslack, path_=core_paths.DATA_PATH, outslack=''):

    """Unpack and extract file to target folder

    inslack:
        Part of file path
        string: looks like this/that.zip

    path_:
        Base placement of data files
        string: default DATA_PATH

    outslack:
        Part of folder path
        string: looks like thisfolder
    """

    dict_of_extention = {'.zip': ZipUnpacker(inslack, path_, outslack)}
    process = checker(inslack, path_, dict_of_extention)
    check_the_output(process).unpack()


def dumper(dump_list=None, path_=core_paths.DUMP_PATH, inslack='default', method='shelve', task=None):

    """Save and open data with serialise tools. 
    Now available:
    - shelve
    
    param dump_list: 
        List or tuple with objects for saving
        list, tuple: default None
    
    inslack:
        Part of file path
        string: looks like thisisnameoffile

    path_:
        Base placement of data files
        string: default DUMP_PATH

    param method: 
        Method of serialisation
        string: default 'shelve'

    param task: 
        Type of operation. 's' for saving, 'o' for opening
        string: default None
    """

    if method == 'shelve':
        dumped = ShelveDump(path_, inslack)
    else:
        print('Wrong method')

    if task == 's':
        try: 
            dumped.dump(dump_list)
        except NameError:
            print("Objects can't be saved")
    elif task == 'o':
        try: 
            return dumped.undump()
        except NameError:
            print("Objects can't be extracted")
    else:
        print("No one object are dumped. Set task 'o' for opening or set task 's' for saving.")


if __name__ == "__main__":
    
    Processor(path_=core_paths.DATA_PATH_TEST).source()
    print('source.... ok')

    loaded = Loader(path_=core_paths.DATA_PATH_TEST, inslack='titanic.csv').load()
    if loaded is True:
        print(loaded.head(1))
        print('load .... ok')
    else:
        print('load .... None')

    unpacker('titanic.zip', path_=core_paths.DATA_PATH_TEST, outslack=core_paths.DATA_OUTPUT_TEST)
    print('unpacker .... ok')

    dumper(dump_list=[loaded], path_=core_paths.DUMP_PATH_TEST, task='s')
    print('dumper save .... ok')

    loaded, = dumper(path_=core_paths.DUMP_PATH_TEST, task='o')
    if loaded is True:
        print(loaded.head(1))
        print('dumper load .... ok')
    else:
        print('dumper load .... None')
