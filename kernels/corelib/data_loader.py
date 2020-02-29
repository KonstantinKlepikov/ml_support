import numpy as np
import pandas as pd
import os
import csv
import codecs
from zipfile import ZipFile
from .core_paths import DATA_PATH, DATA_EXT


class DataLoader:
    """
    Class provide method constructor for unpacking files
    """
    def __init__(self, coding, path_ex, file_n):
        self.coding = coding
        self.path_ex = path_ex
        self.file_n = file_n
        self.handler = None

class csvLoader(DataLoader):
    """
    Class provide method for open *.csv file
    """
    def dictLoader(self):
        with open(self.path_ex, 'r', encoding=self.coding) as file_open:
            if self.handler:
                result = self.handler.go_to_data(file_open)
                return self.file_n, result

class zipLoader(DataLoader):
    """
    Class provide method for unpac .zip archive and open *.csv files
    """
    def dictLoader(self):
        with ZipFile(self.path_ex, 'r') as g:
            for file_name in g.namelist():
                if file_name.endswith('.csv'):
                    with g.open(file_name, 'r') as file_open:
                        if self.handler:
                            result = self.handler.go_to_data(file_open)
                            return self.file_n, result


class DataHandler:
    """
    Class provide method constructor for display and extract .csv data
    """
    def __init__(self):
        self.sep = ','
        self.index_col = None
        self.dtype = None

    def source_tree(self, path, _ext=DATA_EXT):
        """
        Looking for source tree and return dict, where keys are file or directory name, vakues are path to file or dir
        """
        names = os.listdir(os.path.realpath(path))
        std = {}

        for name in names:
            fullname = os.path.join(os.path.realpath(path), name)
            if os.path.isfile(fullname):
                if os.path.splitext(fullname)[1] in _ext:
                    std[name] = fullname
            elif os.path.isdir(fullname):
                if self.source_tree(fullname):
                    std.update(self.source_tree(fullname))

        return std

class DataExtractor(DataHandler):
    """
    Class provide method for extract .csv data to Pandas dataframe
    """
    def go_to_data(self, file_open):
        try:
            data_ex = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype)
        except TypeError:
            print("'{}' is wrong name for parsing of column".format(self.index_col))
            data_ex= None
            
        return data_ex

class DataViewer(DataHandler):
    """
    Class provide method for view .csv data
    """
    def go_to_data(self, file_open):

        data_vi = ''
        for num, line in enumerate(file_open):
            #TODO print names for opened files
            try:
                data_vi += line.decode()
            except (UnicodeDecodeError, AttributeError):
                data_vi += line
            if num >= 5:
                break
        return data_vi

class DataSourcer(DataHandler):
    """
    Class provide method for view list of files in a path
    """
    def go_to_data(self, path):
        s_tree = super(DataSourcer, self).source_tree(path)
        for key, val in s_tree.items():
            print('{0} ..... {1}'.format(key, val))


def loader(mode, path=DATA_PATH, data_for_load=None):
    """
    Unpack data, view .csv files or return pandas dataframe
    
    Parameters
    ----------

    :param mode:
    Mode of function. 
    Available:
    'tree' show  available files
    'view' show first five strings of data files
    'extract' extract data into dict of objects
        string

    :param path:
    Current path to folder with data
        string, default DATA_PATH

    :param data_for_load:
    Dictionary where keys are file names and values are dicts of parameters.
    Looks like {'this.csv': {'sep': ',', 'coding': 'utf-8'}} etc.
    If None all files are loaded wit default parameters.
        dict, default None

    Parameters are available:

    :sep:
    Delimiter to use
        string, default ','

    :index_col:
    Column to use as the row labels of the DataFrame, either given as string name or column index.
    If a sequence of int/str is given, a MultiIndex is used.
    Note: index_col=False can be used to force pandas to not use the first column as the index, e.g. when 
    you have a malformed file with delimiters at the end of each line.
        int, str, sequence of int/str, or False, default None

    :dtype: Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
    Use str or object together with suitable na_values settings to preserve and not interpret dtype.
    If converters are specified, they will be applied INSTEAD of dtype conversion
    As example looks like 'dtype': {'assigned_day': np.float64}
        dict, default None

    :coding: Encoding to - use for UTF when reading/writing (ex. ‘utf-8’)
        str, default None

    Return
    ------

    For mode 'extract' dict with pandas dataframe objects

    Future
    ------

    - Code/decode checking for .zip
    - more formats
    - time stamps converter
    """
    def load_process(source, for_load, mode, call_to_ext):
        """
        Function for load files with different scenarius
        """
        data_dict = {}
        try:
            intercept = list(set(source.keys()).intersection(set(for_load.keys())))
        except AttributeError:
            intercept = list(source.keys())

        for file_n in intercept:

            #set parameters for load
            if for_load:
                call_to_ext.sep = for_load[file_n].get('sep', ',')
                coding = for_load[file_n].get('sep')
                call_to_ext.index_col = for_load[file_n].get('index_col')
                call_to_ext.dtype = for_load[file_n].get('dtype')
            else:
                coding = None

            path_ex = source[file_n]
            #inser new method of files opening
            data_ext = {
                '.csv': csvLoader(coding, path_ex, file_n),
                '.zip': zipLoader(coding, path_ex, file_n)
            }

            loaded = None
            for item in data_ext.items():
                if os.path.splitext(path_ex)[1] == item[0]:
                    stack = item[1]
                    stack.handler = call_to_ext
                    loaded = stack.dictLoader()
                    data_dict[loaded[0]] = loaded[1]

        if data_dict:
            return data_dict

    #insert new method of data observation
    if mode == 'extract':
        call_to_ext = DataExtractor()
        source = call_to_ext.source_tree(path)
        data_dict = load_process(source, data_for_load, mode, call_to_ext)
        print('List of extracted files:')
        for key in data_dict:
            print(key)
        return data_dict

    elif mode == 'view':
        call_to_ext = DataViewer()
        source = call_to_ext.source_tree(path)
        data_dict = load_process(source, data_for_load, mode, call_to_ext)
        print('First five lines of viewed files:\n')
        for key in data_dict:
            print('{0}\n-----\n{1}'.format(key, data_dict[key]))

    elif mode == 'tree':
        call_to_ext = DataSourcer()
        call_to_ext.go_to_data(path)
        
    else:
        print('Wrong mode')