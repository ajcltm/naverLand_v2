from abc import ABCMeta, abstractmethod
import pymysql

class IRawDataset(metaclass=ABCMeta):

    def get_rawDataset(self):
        pass

class IPickedDataset(metaclass=ABCMeta):

    def get_pickedDataset(self):
        pass

class IDumper(metaclass=ABCMeta):

    def __init__(self, folder_path, db_name):
        self.db = pymysql.connect(host='192.168.35.243', port=3306, user='ajcltm', passwd='2642805Ab!', db=db_name, charset='utf8mb4')
        self.folder_path = folder_path

    @abstractmethod
    def insert_value(self):
        pass

class IInsertPipeline(metaclass=ABCMeta):

    def __init__(self, IRawDataset, IPickedDataset, IDumper):
        self.rawDataset = IRawDataset
        self.pickedDataset = IPickedDataset
        self.dumper = IDumper

    def execute(self):
        pass