from db.idumper import IRawDataset, IPickedDataset, IDumper, IInsertPipeline
from db import utils
import os
import _pickle as pickle
from typing import Iterable, List, Dict
from pydantic import BaseModel, ValidationError
from tqdm import tqdm

class GuModel(BaseModel):

    guNo:str
    guName:str
    cityNo:str


class RawDatasetForGu(IRawDataset):

    def __init__(self, folder_path):
        self.folder_path = folder_path

    def get_key_from_fileName(self, fileName):
        return fileName.split('.')[0].split('_')[-1]

    def open_file_and_get_rawData(self, file):
        file_path = self.folder_path.joinpath(file)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(file)
        return {key: data}

    def get_rawDataset(self, file_list:List[str]):
        return (self.open_file_and_get_rawData(file) for file in tqdm(file_list))


class PickedDatasetForGu(IPickedDataset):

    def __init__(self):
        self.error_log = []

    def get_pickedDataset(self, rawDataset:Iterable[Dict])->Iterable[BaseModel]:
        model_dataset=[]
        for rawData in rawDataset:
            for cityNo, dataDict in rawData.items():
                gu_dataset = dataDict.get('regionList')
                if not gu_dataset:
                    self.error_log.append({cityNo:'fail to get the regionList'})                
                for gu_data in gu_dataset:        
                    guNo = gu_data.get('cortarNo')
                    guName = gu_data.get('cortarName')
                    try:
                        model = GuModel(guNo=guNo, guName=guName, cityNo=cityNo)
                    except ValidationError as e:
                        self.error.append(e.json())        
                    model_dataset.append(model)
        return model_dataset

class DumperForGu(IDumper):

    def insert_value(self, pickedDataset:List[BaseModel], commit:bool)->None:
        value_parts = utils.InsertFormatter().get_values_parts(pickedDataset)
        sql = f"insert into gu values {value_parts}"
        self.db.cursor().execute(sql)
        if commit:
            self.db.commit()


class InsertPipelineForGu(IInsertPipeline):

    def __init__(self, IRawDataset, IPickedDataset, IDumper, file_list):
        super().__init__(IRawDataset, IPickedDataset, IDumper)
        self.file_list = file_list

    def execute(self, commit):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        pickedDataset = self.pickedDataset.get_pickedDataset(rawDataset)
        self.dumper.insert_value(pickedDataset, commit)

class GuDumper:

    def __init__(self, folder_path, db_name):
        self.folder_path = folder_path
        self.db_name = db_name
        
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
            
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 1)

    def execute(self, commit=True):
        r = RawDatasetForGu(self.folder_path)
        p = PickedDatasetForGu()
        d = DumperForGu(self.folder_path, self.db_name)

        for file_list in self.chunked_file_list:
            i = InsertPipelineForGu(r, p, d, file_list)
            i.execute(commit)

