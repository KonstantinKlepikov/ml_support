import numpy as np
import pandas as pd
import os
import csv
import codecs
from zipfile import ZipFile
from .core_paths import DATA_PATH

class DataLoader:
    """
    Class provide method constructor for unpacking files
    """
    def __init__(self, coding, path_ex, file_list):
        self.coding = coding
        self.path_ex = path_ex
        self.file_list = file_list
        self.handler = None

class csvLoader(DataLoader):
    """
    Class provide method for open *.csv file
    """
    def dictLoader(self):
        with open(self.path_ex, 'r', encoding=self.coding) as file_open:
            filename = os.path.splitext(self.file_list)[0]
            if self.handler:
                result = self.handler.go_to_data(file_open)
                return [filename, result]

class zipLoader(DataLoader):
    """
    Class provide method for unpac .zip archive and open *.csv files
    """
    def dictLoader(self):
        with ZipFile(self.path_ex, 'r') as g:
            for file_name in g.namelist():
                if file_name.endswith('.csv'):
                    with g.open(file_name, 'r') as file_open:
                        filename = os.path.splitext(file_name)[0]
                        if self.handler:
                            result = self.handler.go_to_data(file_open)
                            return [filename, result]


class DataHandler:
    """
    Class provide method constructor for display and extract .csv data
    """
    def __init__(self):
        pass
        # TODO subclass for display list of available files

    def source_tree(self, path):
        """
        Looking for source tree and return dict, where keys are file or directory name, vakues are path to file or dir
        """
        names = os.listdir(os.path.realpath(path))
        source_td = {}

        for name in names:

            fullname = os.path.join(os.path.realpath(path), name)
            if os.path.isfile(fullname):
                source_td[name] = fullname
            elif os.path.isdir(fullname):
                source_tds = {}
                subpath = os.path.join(os.path.realpath(path), name)
                if self.source_tree(subpath):
                    source_tds[name] = self.source_tree(subpath)
                source_td[name] = source_tds

        return source_td

class DataExtractor(DataHandler):
    """
    Class provide method for extract .csv data to Pandas dataframe
    """
    def __init__(self, sep, index_col, dtype):
        self.sep = sep
        self.index_col = index_col
        self.dtype = dtype

    def go_to_data(self, file_open):
        try:
            data_for_opening = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype)
        except TypeError:
            print("'{}' is wrong name for parsing of column".format(self.index_col))
            data_for_opening = None
            
        return data_for_opening

class DataViewer(DataHandler):
    """
    Class provide method for view .csv data
    """
    def go_to_data(self, file_open):

        for num, line in enumerate(file_open):
            try:
                data = line.rstrip().decode()
                print(data)
            except (UnicodeDecodeError, AttributeError):
                print(line.rstrip())
            if num >= 5: break


def loader(mode, path=DATA_PATH, data_for_load=None, sep=',', index_col=None, dtype=None, coding=None):
    """
    Unpack kaggle data, view .csv files or return pandas dataframe
    
    Parameters
    ----------

    :param mode:
    mode of function. Available 'extract' for extracting data into dict of objects
    or 'view' for show first 5 strings of data files
        string

    :param path:
    current path to folder with data
        string, default DATA_PATH constant

    :param data_for_load:
    dict ehere keys are file names and values are dicts of parameters.
    Looks like {'this.csv': {'sep': ',', 'coding': 'utf-8'}} etc.
    If None all files are loaded wit default parameters.
        dict, default None

    For data_for_load parameters are available:

    :sep:
    Delimiter to use
        string, default ','

    :index_col:
    Column to use as the row labels of the DataFrame,
    either given as string name or column index.
    If a sequence of int / str is given, a MultiIndex is used.
    Note: index_col=False can be used to force pandas to not use the first column as the index, e.g. when 
    you have a malformed file with delimiters at the end of each line.
        int, str, sequence of int / str, or False, default False

    :dtype: Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
    Use str or object together with suitable na_values settings to preserve and not interpret dtype.
    If converters are specified, they will be applied INSTEAD of dtype conversion
        dict, default None

    :coding: Encoding to - use for UTF when reading/writing (ex. ‘utf-8’)
        str, default None




    :param sep:
    Delimiter to use
        string, default ','
    
    :param index_col:    
    Column to use as the row labels of the DataFrame,
    either given as string name or column index.

    If a sequence of int / str is given, a MultiIndex is used.

    Note: index_col=False can be used to force pandas to not use the first column as the index, e.g. when 
    you have a malformed file with delimiters at the end of each line.
        int, str, sequence of int / str, or False, default False

    :param dtype: 
    Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
    Use str or object together with suitable na_values settings to preserve and not interpret dtype.
    If converters are specified, they will be applied INSTEAD of dtype conversion
        dict, default None

    :param coding: 
    Encoding to - use for UTF when reading/writing (ex. ‘utf-8’)
        str, default None

    Return
    ------

    Dict with pandas data frame objects for 'extract' mode or dict of None for 'view' mode

    Future
    ------

    - rewrite extractor for checking different headers in different files
    - Code/decode checking for .zip
    - Search deeper in folder
    - other formats
    - time stamps converter
    """
    data_dict = {}
    #insert new method of data observation
    if mode == 'extract':
        call_to_ext = DataExtractor(sep, index_col, dtype)
    elif mode == 'view':
        call_to_ext = DataViewer()
    else:
        print('Wrong mode')

    for file_list in os.listdir(path):
        path_ex = os.path.join(path, file_list)
        #inser new method of files opening
        data_ext = {
            '.csv': csvLoader(coding, path_ex, file_list),
            '.zip': zipLoader(coding, path_ex, file_list)
        }
        loaded = None

        for item in data_ext.items():
            if os.path.splitext(path_ex)[1] == item[0]:
                stack = item[1]
                stack.handler = call_to_ext
                loaded = stack.dictLoader()
                data_dict[loaded[0]] = loaded[1]

    return data_dict