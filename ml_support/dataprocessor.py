import numpy as np
import pandas as pd
import os, csv, shelve, inspect
from zipfile import ZipFile
from zipfile import BadZipfile
from abc import ABC, abstractmethod
import core_paths


class Fabricator(ABC):

    """Base class for fabric methods
    """

    @classmethod
    @abstractmethod
    def _is_check_for(cls, check):
        pass


class Loader(ABC):

    """Base class for data load
    """

    def __init__(self, in_path, index_col, dtype, parse_dates, sep, encoding):
        self.in_path = in_path
        self.index_col = index_col
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.sep = sep
        self.encoding = encoding

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def view(self):
        pass


class Packer(ABC):

    """Base class for pack
    """

    def __init__(self, in_path, out_path):
        self.in_path = in_path
        self.out_path = out_path

    @abstractmethod
    def pack(self):
        pass


class Unpacker(ABC):

    """Base class for unpack
    """

    def __init__(self, in_path, out_path):
        self.in_path = in_path
        self.out_path = out_path

    @abstractmethod
    def unpack(self):
        pass


class Dumper(ABC):

    """Base class for serialize
    """

    def __init__(self, in_path, out_path, dump_list):
        self.in_path = in_path
        self.out_path = out_path
        self.dump_list = dump_list

    @abstractmethod
    def dump(self):
        pass


class Undumper(ABC):

    """Base class for unserialize
    """

    def __init__(self, out_path):
        self.out_path = out_path

    @abstractmethod
    def undump(self):
        pass


class CSVLoader(Loader, Fabricator):


    @classmethod
    def _is_check_for(cls, check):
        return check == '.csv'

    def load(self):

        with open(self.in_path, 'r', encoding=self.encoding) as file_open:
            try:
                data_ex = pd.read_csv(file_open, sep=self.sep, index_col=self.index_col, dtype=self.dtype, parse_dates=self.parse_dates)
            except TypeError:
                print("'{}' is wrong name for parsing of column".format(self.index_col))
                data_ex = None
        
        return data_ex

    def view(self):

        data_vi = ''
        with open(self.in_path, 'r', encoding=self.encoding) as file_open:
            for num, line in enumerate(file_open):
                try:
                    data_vi += line.decode()
                except (UnicodeDecodeError, AttributeError):
                    data_vi += line
                if num >= 5:
                    break

        return data_vi


class ZipUnpacker(Unpacker, Fabricator):


    @classmethod
    def _is_check_for(cls, check):
        return check == '.zip'
    
    def unpack(self):

        with ZipFile(self.in_path, 'r') as g:
            inside = g.infolist()
            for ig in inside:
                try:
                    g.extract(ig, self.out_path)
                    print('{} ... unpacked'.format(ig))
                except BadZipfile:
                    print('{} ... wrong file'.format(ig))


class ShelveDumper(Dumper, Fabricator):


    @classmethod
    def _is_check_for(cls, check):
        return check == 'shelve'

    def dump(self):

        with shelve.open(self.out_path) as s:
            for k, v in enumerate(self.dump_list):
                try:
                    s[str(k)] = v
                    print('Object {0} is dumped to "{1}" objects'.format(k, self.out_path))
                except TypeError:
                    print('Object {0} not dumped - an error occurred'.format(k))


class ShelveUndumper(Undumper, Fabricator):


    @classmethod
    def _is_check_for(cls, check):
        return check == 'shelve'

    def undump(self):

        dict_of_objects = {}
        with shelve.open(self.out_path) as o:
            for k, v in o.items():
                dict_of_objects[k] = v
            return dict_of_objects.values()
