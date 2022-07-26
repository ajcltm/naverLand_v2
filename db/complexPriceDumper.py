from db.idumper import IRawDataset, IPickedDataset, IDumper, IInsertPipeline
from scrap import config
from db import utils
from db import idumper
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional, List, Iterable, Dict
from pydantic import BaseModel, validator, ValidationError
from tqdm import tqdm
from pathlib import Path

class ComplexPriceModel_1(BaseModel):
    complexNo : str
    ptpNo : str
    date : Optional[List[datetime]]
    price : Optional[List[int]]

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


class ComplexPriceModel_2(BaseModel):
    complexNo : str
    ptpNo : str
    date : Optional[datetime]
    price : Optional[int]

    @validator('*', pre=True, always=True)
    def deal_with_none(cls, v, values):
        if not v:
            return None
        return v

class RawDatasetForComplexPrice(IRawDataset):

    def __init__(self, folder_path):
        self.folder_path = folder_path

    def get_key_from_fileName(self, fileName):
        hscpNo=fileName.split('.')[0].split('_')[-2]
        ptpNo=fileName.split('.')[0].split('_')[-1]
        return (hscpNo, ptpNo)

    def open_file_and_get_rawData(self, file):
        file_path = self.folder_path.joinpath(file)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(file)
        yield {key: data}

    def get_rawDataset(self, file_list:List[str])->Iterable[Dict]:      # ex : ['file_name1.pickle', 'file_name2.pickle']
        return (self.open_file_and_get_rawData(file) for file in tqdm(file_list))


class PickedDatasetForComplexPrice(IPickedDataset):

    def get_pickedDataset(self, rawDataset:Iterable[Dict])->Iterable[BaseModel]:
        model1_dataset = []
        for rawData in rawDataset:
            for complexNoAndPtpNo, complexPriceData in list(rawData)[0].items():
                try:
                    model_1 = ComplexPriceModel_1(complexNo=complexNoAndPtpNo[0], ptpNo=complexNoAndPtpNo[1], date=complexPriceData.get('realPriceDataXList')[1:], price=complexPriceData.get('realPriceDataYList')[1:])
                    model1_dataset.append(model_1)
                except ValidationError as e:
                    print(f'validationError {complexNoAndPtpNo}')
                    print(e.json())
        
        model2_dataset = []
        for model1_data in model1_dataset:
            if not model1_data.price:
                model2 = ComplexPriceModel_2(complexNo=model1_data.complexNo, ptpNo=model1_data.ptpNo, date=None, price=None)
                model2_dataset.append(model2)
                continue

            for i in range(0, len(model1_data.price)):
                try:
                    model2 = ComplexPriceModel_2(complexNo=model1_data.complexNo, ptpNo=model1_data.ptpNo, date=model1_data.date[i], price=model1_data.price[i])
                    model2_dataset.append(model2)
                except ValidationError as e:
                    print(f'validationError {complexNoAndPtpNo}')
                    print(e.json())
        return model2_dataset


class DumperForComplexPrice(IDumper):

    def insert_value(self, pickedDataset:List[BaseModel])->None:
        value_parts = utils.InsertFormatter().get_values_parts(pickedDataset)
        sql = f"insert into complexPrice values {value_parts}"
        self.db.cursor().execute(sql)
        self.db.commit()


class InsertPipelineForComplexPrice(IInsertPipeline):

    def __init__(self, IRawDataset, IPickedDataset, IDumper, file_list):
        super().__init__(IRawDataset, IPickedDataset, IDumper)
        self.file_list = file_list

    def execute(self):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        pickedDataset = self.pickedDataset.get_pickedDataset(rawDataset)
        self.dumper.insert_value(pickedDataset)


class ComplexPriceDumper:

    def __init__(self, folder_path, db_name):
        self.folder_path = folder_path
        self.db_name = db_name
        
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
            
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 20)

    def execute(self):
        r = RawDatasetForComplexPrice(self.folder_path)
        p = PickedDatasetForComplexPrice()
        d = DumperForComplexPrice(self.folder_path, self.db_name)

        for file_list in self.chunked_file_list:
            i = InsertPipelineForComplexPrice(r, p, d, file_list)
            i.execute()

if __name__ == '__main__':
    folder_path = Path('F:').joinpath('data', 'naverLand', '220714', '5. complexPrice')
    ComplexPriceDumper(folder_path, 'naverland').execute()

    # file_list = ['hscpNo_ptpNo_3327_2.pickle']
    # f = folder_path.joinpath(file_list[0])
    # with open(f, mode='rb') as fr:
    #     data = pickle.load(fr)
    # print(data)