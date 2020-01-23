import shelve
import os

DUMP_PATH = '../kernels/dumped_data/'

def shelve_dump(dump_list=None, path='default', task=None):

    """
    Save and open data series with shelve
    
    Parameters
    ----------
    :param dump_list: list or tuple with objects for saving
        list, tuple
    
    :param path: current path name to folder with data
        string, default 'default'

    :param task: type of operation. 's' for saving, 'o' for opening
        string, default None

    """
    ospath = os.path.realpath(DUMP_PATH + path)

    if task == 's':
        with shelve.open(ospath) as s:
            for k, v in enumerate(dump_list):
                try:
                    s[str(k)] = v
                    print('Object {0} is dumped to "{1}" objects'.format(k, path))
                except:
                    print('Object {} not dumped - an error occurred'.format(k))
    elif task == 'o':
        dict_of_objects = {}
        with shelve.open(ospath) as o:
            for k, v in o.items():
                dict_of_objects[k] = v
            return dict_of_objects.values()
    else:
        print("Operation not started. Set task 'o' for opening or set task 's' for saving.")