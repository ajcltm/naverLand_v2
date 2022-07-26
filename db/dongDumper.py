from db import idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from pydantic import BaseModel

class DongModel(BaseModel):
    dongNo:str
    dongName:str
    guNo:str


class DongDumper(idumper.Dumper):

    def get_key_from_fileName(self, fileName):
        return fileName.split('.')[0].split('_')[-1]

    def get_data(self):
        file_list = os.listdir(self.folder_path)
        container = []
        for file in file_list:
            file_path = self.folder_path.joinpath(file)
            with open(file_path, mode='rb') as fr:
                data = pickle.load(fr)
            key = self.get_key_from_fileName(file)
            container.append({key: data})
        return container

    def get_subData(self):
        data = self.get_data()
        lst = []
        for gu_dict in data:
            for gu_no, gu_data in gu_dict.items():
                for dong_data in gu_data['regionList']:
                    lst.append(DongModel(dongNo=dong_data['cortarNo'], dongName=dong_data['cortarName'], guNo=gu_no))
        return lst

    def insert_value(self):
        data_list = self.get_subData()
        value_parts = utils.InsertFormatter().get_values_parts(data_list)
        sql = f"insert into dong values {value_parts}"
        self.db.cursor().execute(sql)
        self.db.commit()

if __name__ == '__main__':
    DongDumper(config.main_path.joinpath('1. dong')).insert_value()
    
