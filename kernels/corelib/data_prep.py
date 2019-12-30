import numpy as np
import pandas as pd
import os
from zipfile import ZipFile

DATA_PATH = os.path.realpath('../input')
DUMP_PATH = os.path.realpath('../kernels/loaded_data')


class DataLoader:

    """
    Class provide method constructor for unpacking files

    """    
    def __init__(self, path, index_col, dtype, coding, path_ex, file_list):

        self.path = path
        self.index_col = index_col
        self.dtype = dtype
        self.coding = coding
        self.path_ex = path_ex
        self.file_list = file_list

    def extractor(self, file_open):
    
        """
        Check the headers of .csv file for compliance with the index. If index is wrong, None is returned

        """
        try:
            data_for_opening = pd.read_csv(file_open, index_col=self.index_col, dtype=self.dtype)
            print(data_for_opening)
        except:
            print("'{0}' is wrong name for parsing of column".format(self.index_col))
            data_for_opening = None

        return data_for_opening

class csvLoader(DataLoader):

    """
    Class provide method for open *.csv files and save it to dict of DataFrame objects

    """
    def dictLoader(self):

        # Open and save *.csv file
        with open(self.path_ex, 'r', encoding=self.coding) as file_open:
            filename = os.path.splitext(self.file_list)[0]
            return [filename, super(csvLoader, self).extractor(file_open)]

class zipLoader(DataLoader):

    """
    Class provide method for unpac *.csv files from .zip archive and save it to dict of DataFrame objects

    """
    def dictLoader(self):

        # Open .zip and save *.csv file
        with ZipFile(self.path_ex, 'r') as g:
            for file_name in g.namelist():
                if file_name.endswith('.csv'):
                    with g.open(file_name) as file_open:
                        filename = os.path.splitext(file_name)[0]
                        return [filename, super(zipLoader, self).extractor(file_open)]


def loader(path=DATA_PATH, index_col=False, dtype=None, coding=None):
    
    """
    Unpack kaggle data, then return pandas dataframe (function prototype)
    
    Parameters
    ----------
    :param path: current path to folder with data
        string
    
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
    - diferent delimiters
    - other formats
    - time stamps converter

    """
    data_dict = {}

    for file_list in os.listdir(path):
        path_ex = os.path.join(path, file_list)
        data_ext = {
            '.csv': csvLoader(path, index_col, dtype, coding, path_ex, file_list),
            '.zip': zipLoader(path, index_col, dtype, coding, path_ex, file_list)
        }
        loaded = None

        for item in data_ext.items():
            if os.path.splitext(path_ex)[1] == item[0]:
                csvload = item[1]
        
                loaded = csvload.dictLoader()

                if loaded:
                    data_dict[loaded[0]] = loaded[1]

    return data_dict


def reduce_mem_usage(df, verbose=True):

    """
    Reduce numeric 
    
    Parameters
    ----------
    :param df: pandas data frame
        pd.DataFrame object

    Return
    ------

    Pandas data frame object

    Future
    ------

    - optimisation by transfer float to int
    - reduce objects

    """
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024**2

    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
                    
    end_mem = df.memory_usage().sum() / 1024**2
    if verbose: print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    
    return df


def reduce_obj_mem_usage(df, verbose=True):

    """
    Reduce object. Return new data frame, containing only columns with dtype object.
    Columns with number of unique values, that is no more than 50%, recieve subtype category
    
    Parameters
    ----------
    :param df: pandas data frame
        pd.DataFrame object

    Return
    ------

    New pandas data frame, which contains only object and categorial dtype columns.
    All other columns is droped.

    Future
    ------

    - all columns return

    """    
    df = df.select_dtypes(include=['object']).copy()

    df.describe()
    start_mem = df.memory_usage().sum() / 1024**2

    converted = pd.DataFrame()

    for col in df.columns:
        unic = len(df[col].unique())
        total = len(df[col])
        if unic / total < 0.5:
            converted.loc[:,col] = df[col].astype('category')
        else:
            converted.loc[:,col] = df[col]

    converted.describe()
    end_mem = converted.memory_usage().sum() / 1024**2

    if verbose: print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))

    return converted


def search_func(data, *cols):

    """
    Function return dictionary of the form: 'value': index, that can be used for 
    mapping in ordered feature encoding estimators
    
    Parameters
    ----------
    :param data: pandas data frame
        pd.DataFrame object
    
    :param cols: list of columns, where function search for unical ordered value
        list, tuple

    Return
    ------

    List of dicts, where keys are names of values for ordered encoding, and values are position in order
    
    """
    full_map = []

    for i in cols:
        mapping = {}
        for idx, val in enumerate(pd.unique(sorted(data[i]))):
            mapping[val] = idx
        full_map.append(mapping)
        
    return full_map