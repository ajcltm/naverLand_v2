from db.idumper import IRawDataset, IPickedDataset, IDumper, IInsertPipeline
from db import utils
import os
import _pickle as pickle
from typing import Iterable, List, Dict
from pydantic import BaseModel, ValidationError
from tqdm import tqdm

class DongModel(BaseModel):
    dongNo:str
    dongName:str
    guNo:str


class RawDatasetForDong(IRawDataset):

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


class PickedDatasetForDong(IPickedDataset):

    def __init__(self):
        self.error_log = []

    def get_pickedDataset(self, rawDataset:Iterable[Dict])->Iterable[BaseModel]:
        model_dataset=[]
        for rawData in rawDataset:
            for guNo, dataDict in rawData.items():
                dong_dataset = dataDict.get('regionList')
                if not dong_dataset:
                    self.error_log.append({guNo:'fail to get the regionList'})                
                for dong_data in dong_dataset:        
                    dongNo = dong_data.get('cortarNo')
                    dongName = dong_data.get('cortarName')
                    try:
                        model = DongModel(dongNo=dongNo, dongName=dongName, guNo=guNo)
                    except ValidationError as e:
                        self.error.append(e.json())        
                    model_dataset.append(model)
        return model_dataset

class DumperForDong(IDumper):

    def insert_value(self, pickedDataset:List[BaseModel], commit:bool)->None:
        value_parts = utils.InsertFormatter().get_values_parts(pickedDataset)
        sql = f"insert into dong values {value_parts}"
        self.db.cursor().execute(sql)
        if commit:
            self.db.commit()


class InsertPipelineForDong(IInsertPipeline):

    def __init__(self, IRawDataset, IPickedDataset, IDumper, file_list):
        super().__init__(IRawDataset, IPickedDataset, IDumper)
        self.file_list = file_list

    def execute(self, commit):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        pickedDataset = self.pickedDataset.get_pickedDataset(rawDataset)
        self.dumper.insert_value(pickedDataset, commit)

class DongDumper:

    def __init__(self, folder_path, db_name):
        self.folder_path = folder_path
        self.db_name = db_name
        
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
            
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 1)

    def execute(self, commit=True):
        r = RawDatasetForDong(self.folder_path)
        p = PickedDatasetForDong()
        d = DumperForDong(self.folder_path, self.db_name)

        for file_list in self.chunked_file_list:
            i = InsertPipelineForDong(r, p, d, file_list)
            i.execute(commit)

    
