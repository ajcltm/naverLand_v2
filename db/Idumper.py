from abc import ABCMeta, abstractmethod
import pymysql

class Dumper(metaclass=ABCMeta):

    def __init__(self, folder_path):
        self.db = pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='naverland', charset='utf8mb4')
        self.folder_path = folder_path

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_key_from_fileName(self, fileName):
        pass

    @abstractmethod
    def get_subData(self):
        pass

    @abstractmethod
    def insert_value(self):
        pass




