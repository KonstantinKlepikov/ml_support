import numpy as np
import pandas as pd
import os
from zipfile import ZipFile

def loader(path, index_col=False, dtype=None, encoding=None):

    """
    Unpack kaggle zip-data, then return dict of pd.data
    
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

    Future
    ------

    - Code/decode checking for .zip
    - Search deeper in folder

    """
    
    data_dict = {}

    for i in os.listdir(path):

        path_ex = os.path.join(path, i)

        if os.path.splitext(path_ex)[1] == ".zip":
            with ZipFile(path_ex, 'r') as g:
                for file_name in g.namelist():
                    if file_name.endswith('.csv'):
                        with g.open(file_name) as h:
                            filename = os.path.splitext(file_name)[0]
                            data_dict[filename] = pd.read_csv(h, index_col=index_col, dtype=dtype)

        elif os.path.splitext(path_ex)[1] == ".csv":
            with open(path_ex, 'r', encoding=encoding) as g:
                filename = os.path.splitext(i)[0]
                data_dict[filename] = pd.read_csv(g, index_col=index_col, dtype=dtype)

    return data_dict

def reduce_mem_usage(df, verbose=True):

    """
    Reduse numeric 
    
    Parameters
    ----------
    :param df: pandas data frame
        pd.DataFrame object

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
    
    """

    full_map = []

    for i in cols:
        mapping = {}
        for idx, val in enumerate(pd.unique(sorted(data[i]))):
            mapping[val] = idx
        full_map.append(mapping)
        
    return full_map