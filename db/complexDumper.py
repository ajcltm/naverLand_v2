from db import idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator

class ComplexModel(BaseModel):
    complexNo:str
    complexName:str
    dongNo:str
    realEstateTypeCode:str
    cortarAddress:str
    detailAddress:str
    totalHouseholdCount:int
    totalBuildingCount:int
    highFloor:int
    lowFloor:int
    useApproveYmd:Optional[datetime]

    @validator('useApproveYmd', pre=True, always=True)
    def deal_with_none(cls, v, values):
        if not v:
            return None
        elif len(v)==8:
            return datetime.strptime(v, '%Y%m%d')
        elif len(v)==6:
            return datetime.strptime(v, '%Y%m')
        elif len(v)==4:
            return datetime.strptime(v, '%y%m') 

class ComplexDumper(idumper.Dumper):

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
        for dong_dict in data:
            for dong_no, dong_data in dong_dict.items():
                for complex_data in dong_data['complexList']:
                    lst.append(ComplexModel(
                        complexNo=complex_data.get('complexNo'), 
                        complexName=complex_data.get('complexName'), 
                        dongNo=dong_no,
                        realEstateTypeCode=complex_data.get('realEstateTypeCode'),
                        cortarAddress=complex_data.get('cortarAddress'),
                        detailAddress=complex_data.get('detailAddress'),
                        totalHouseholdCount=complex_data.get('totalHouseholdCount'),
                        totalBuildingCount=complex_data.get('totalBuildingCount'),
                        highFloor=complex_data.get('highFloor'),
                        lowFloor=complex_data.get('lowFloor'),
                        useApproveYmd=complex_data.get('useApproveYmd')
                        ))
        return lst

    def insert_value(self):
        data_list = self.get_subData()
        value_parts = utils.InsertFormatter().get_values_parts(data_list)
        sql = f"insert into complex values {value_parts}"
        self.db.cursor().execute(sql)
        self.db.commit()

if __name__ == '__main__':
    ComplexDumper(config.main_path.joinpath('2. complex')).insert_value()

    
