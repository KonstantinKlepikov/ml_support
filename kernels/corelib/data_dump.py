import shelve
import os

DUMP_PATH = os.path.realpath('../kernels/loaded_data/loaded_data')

def shelve_dump(dump_list=None, path=DUMP_PATH, task=None):

    """
    Save and open data series with shelve
    
    Parameters
    ----------
    :param dump_list: list or tuple with objects for saving
        kist, tuple
    
    :param path: current path to folder with data
        string, default DUMP_PATH

    :param task: type of operation. 's' for saving, 'o' for opening
        string, default None

    Future
    ------

    - set name for dumped/loaded structure

    """

    if task == 's':
        with shelve.open(path) as s:
            for k, v in enumerate(dump_list):
                try:
                    s[str(k)] = v
                    print('Object {} is dumped'.format(k))
                except:
                    print('Object {} not dumped - an error occurred'.format(k))
    elif task == 'o':
        dict_of_objects = {}
        with shelve.open(path) as o:
            for k, v in o.items():
                dict_of_objects[k] = v
            return dict_of_objects.values()
    else:
        print("Operation not started. Set task 'o' for opening or set task 's' for saving.")