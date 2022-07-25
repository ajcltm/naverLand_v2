from db import Idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, validator, ValidationError
from tqdm import tqdm
import pandas as pd

class ComplexPriceModel(BaseModel):
    complexNo : str
    ptpNo : str
    date : List[datetime]
    price : List[int]
    pct_change : Optional[float]


    @validator('*', pre=True, always=True)
    def deal_with_none(cls, v, values):
        if not v:
            return None
        return v

    @validator('date', pre=True, always=True)
    def datetime_change(cls, v, values):
        if not v:
            return None
        return [datetime.strptime(i, '%Y-%m-%d') for i in v]

    @validator('pct_change', always=True)
    def pct_chagne(cls, v, values):
        if not values['price']:
            return None
        elif len(values['price'])==1:
            return None
        df = pd.DataFrame({'date':values['date'], 'price':values['price']})
        # print(df)
        print(df.groupby('date').last().pct_change())
        return (values['price'][-1] - values['price'][-2])/values['price'][-1]


class ComplexPriceDumper(Idumper.Dumper):

    def __init__(self, file_path):
        super().__init__(file_path)
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 20)

    def get_key_from_fileName(self, fileName):
        hscpNo=fileName.split('.')[0].split('_')[-2]
        ptpNo=fileName.split('.')[0].split('_')[-1]
        return (hscpNo, ptpNo)

    def open_file_get_data(self, file):
        file_path = self.folder_path.joinpath(file)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(file)
        yield {key: data}

    def get_data(self, file_list):
        return (self.open_file_get_data(file) for file in tqdm(file_list))

    def get_subData(self, file_list):
        data = self.get_data(file_list)
        lst = []
        for complexPrice_dict in data:
            for complexNoAndPtpNo, complexPriceData in list(complexPrice_dict)[0].items():
                try:
                    model = ComplexPriceModel(
                                complexNo=complexNoAndPtpNo[0],
                                ptpNo=complexNoAndPtpNo[1],
                                date=complexPriceData['realPriceDataXList'][1:],
                                price=complexPriceData['realPriceDataYList'][1:])
                    lst.append(model
                        )
                    # print(model)
                except ValidationError as e:
                    print(f'validationError {complexNoAndPtpNo}')
                    print(e.json())
        return lst

    def insert_value(self):
        
        for file_list in self.chunked_file_list:
            data_list = self.get_subData(file_list)
            value_parts = utils.InsertFormatter().get_values_parts(data_list)
            sql = f"insert into articleInfo values {value_parts}"
            self.db.cursor().execute(sql)
            self.db.commit()

if __name__ == '__main__':
    # ComplexPriceDumper(config.main_path.joinpath('5. complexPrice')).insert_value()
    # import pymysql
    # db = pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='naverland', charset='utf8')
    # sql = 'select * from article'
    # c = db.cursor()
    # c.execute(sql)
    # print(len(list(c.fetchall())))
    file_list = ['hscpNo_ptpNo_23_3.pickle']
    ComplexPriceDumper(config.main_path.joinpath('5. complexPrice')).get_subData(file_list)
