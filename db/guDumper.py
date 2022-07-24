from db import Idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from pydantic import BaseModel

class GuModel(BaseModel):

    guNo:str
    guName:str
    cityNo:str


class GuDumper(Idumper.Dumper):

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
        for city_dict in data:
            for city_no, city_data in city_dict.items():
                for gu_data in city_data['regionList']:
                    lst.append(GuModel(guNo=gu_data['cortarNo'], guName=gu_data['cortarName'], cityNo=city_no))
        return lst

    def insert_value(self):
        data_list = self.get_subData()
        value_parts = utils.InsertFormatter().get_values_parts(data_list)
        sql = f"insert into gu values {value_parts}"
        self.db.cursor().execute(sql)
        self.db.commit()

if __name__ == '__main__':
    GuDumper(config.main_path.joinpath('0. gu')).insert_value()
