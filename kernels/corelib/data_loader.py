import numpy as np
import pandas as pd
import os
import csv
from zipfile import ZipFile
from core_paths import DATA_PATH

class DataLoader:
    """
    Class provide method constructor for unpacking files

    """
    def __init__(self, path, coding, path_ex, file_list):
        self.path = path
        self.coding = coding
        self.path_ex = path_ex
        self.file_list = file_list

class csvLoader(DataLoader):
    """
    Class provide method for open *.csv files and save it to dict of DataFrame objects

    """
    def dictLoader(self):
        with open(self.path_ex, 'r', encoding=self.coding) as file_open:
            filename = os.path.splitext(self.file_list)[0]
            return [filename, file_open]

class zipLoader(DataLoader):
    """
    Class provide method for unpac *.csv files from .zip archive and save it to dict of DataFrame objects

    """
    def dictLoader(self):
        with ZipFile(self.path_ex, 'r') as g:
            for file_name in g.namelist():
                if file_name.endswith('.csv'):
                    with g.open(file_name) as file_open:
                        filename = os.path.splitext(file_name)[0]
                        return [filename, file_open]

class DataHandler:
    """
    Class provide method constructor for display data

    """
    def __init__(self, sep, file_open):
        self.sep = sep
        self.file_open = file_open

class DataExtractor(DataHandler):

    def __init__(self, sep, file_open, index_col, dtype):
        super().__init__(sep, file_open)
        self.index_col = index_col
        self.dtype = dtype

    def extractor(self):
        """
        Check the headers of .csv file for compliance with the index. If index is wrong, None is returned

        """
        try:
            data_for_opening = pd.read_csv(self.file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype)
        except:
            print("'{}' is wrong name for parsing of column".format(self.index_col))
            data_for_opening = None
            
        return data_for_opening

class DataViewer(DataHandler):

    def viewer(self):
        """
        Viewe first five strings of .csv file

        """
        try:
            reader = csv.reader(self.file_open, delimiter=self.sep)
            for i, row in enumerate(reader):
                print(row)
                if(i>=5): break
        except:
            print("File cant be opened")

def loader(mode, path=DATA_PATH, sep=',', index_col=False, dtype=None, coding=None):    
    """
    Unpack kaggle data, then return pandas dataframe (function prototype)
    
    Parameters
    ----------
    :param path: current path to folder with data
        string

    :param sep: Delimiter to use
        str, default ','
    
    :param index_col: Column to use as the row labels of the DataFrame, either given as string name or column index.
    If a sequence of int / str is given, a MultiIndex is used.
    Note: index_col=False can be used to force pandas to not use the first column as the index, e.g. when 
    you have a malformed file with delimiters at the end of each line. 
        int, str, sequence of int / str, or False, default False

    :param dtype: Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’} 
    Use str or object together with suitable na_values settings to preserve and not interpret dtype. 
    If converters are specified, they will be applied INSTEAD of dtype conversion
        dict, default None

    :param encoding: Encoding to use for UTF when reading/writing (ex. ‘utf-8’)
        str, default None

    Return
    ------

    Dict with pandas data frame objects

    Future
    ------

    - rewrite extractor for checking different headers in different files
    - Code/decode checking for .zip
    - Search deeper in folder
    - other formats
    - time stamps converter

    """
    data_dict = {}

    for file_list in os.listdir(path):
        path_ex = os.path.join(path, file_list)
        data_ext = {
            '.csv': csvLoader(path, coding, path_ex, file_list),
            '.zip': zipLoader(path, coding, path_ex, file_list)
        }
        loaded = None

        for item in data_ext.items():
            if os.path.splitext(path_ex)[1] == item[0]:
                csvload = item[1]
        
                loaded = csvload.dictLoader()
                if loaded:
                    if mode == 'extract':
                        mode_ext = DataExtractor(sep, loaded[1], index_col, dtype)
                        data_dict[loaded[0]] = mode_ext.extractor()
                    elif mode == 'view':
                        mode_ext = DataViewer(sep, loaded[1])
                        mode_ext.viewer()

    return data_dict