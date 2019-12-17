import shelve

def shelve_dump(dump_dict, path, task=None):

    """
    Save and open data series with shelve
    
    Parameters
    ----------
    :param dump_dict: dictionary, where key is file names, and value is an object for saving
        dict
    
    :param path: current path to folder with data
            string

    :param task: type of operation. 's' for saving, 'o' for opening
        str, default None

    """

    if task == 'o':
        with shelve.open(path) as o:
            for k, v in dump_dict.items():
                o[k] = v
                print('Object {0} is dumped'.format(o[k]))
    elif task == 's':
        with shelve.open(path) as s:
            for k, v in dump_dict.items():
                v = s[k]
                print('Object {0} is opened'.format(s[k]))
    else:
        print("Operation not started. Set task 'o' for opening or set task 's' for saving.")