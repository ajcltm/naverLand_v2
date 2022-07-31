from db.idumper import IRawDataset, IPickedDataset, IDumper, IInsertPipeline
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Iterable, List, Dict, Optional
from pydantic import BaseModel, validator, ValidationError
from tqdm import tqdm

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


class RawDatasetForComplex(IRawDataset):

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


class PickedDatasetForComplex(IPickedDataset):

    def __init__(self):
        self.error_log = []

    def get_pickedDataset(self, rawDataset:Iterable[Dict])->Iterable[BaseModel]:
        model_dataset=[]
        for rawData in rawDataset:
            for dongNo, dataDict in rawData.items():
                complex_dataset = dataDict.get('complexList')
                if not complex_dataset:
                    self.error_log.append({dongNo:'fail to get the complexList'})                
                for complex_data in complex_dataset:        
                    try:
                        model = ComplexModel(
                            complexNo=complex_data.get('complexNo'), 
                            complexName=complex_data.get('complexName'), 
                            dongNo=dongNo,
                            realEstateTypeCode=complex_data.get('realEstateTypeCode'),
                            cortarAddress=complex_data.get('cortarAddress'),
                            detailAddress=complex_data.get('detailAddress'),
                            totalHouseholdCount=complex_data.get('totalHouseholdCount'),
                            totalBuildingCount=complex_data.get('totalBuildingCount'),
                            highFloor=complex_data.get('highFloor'),
                            lowFloor=complex_data.get('lowFloor'),
                            useApproveYmd=complex_data.get('useApproveYmd')
                        )
                    except ValidationError as e:
                        self.error.append(e.json())        
                    model_dataset.append(model)
        return model_dataset

class DumperForComplex(IDumper):

    def insert_value(self, pickedDataset:List[BaseModel], commit:bool)->None:
        value_parts = utils.InsertFormatter().get_values_parts(pickedDataset)
        sql = f"insert into complex values {value_parts}"
        self.db.cursor().execute(sql)
        if commit:
            self.db.commit()


class InsertPipelineForComplex(IInsertPipeline):

    def __init__(self, IRawDataset, IPickedDataset, IDumper, file_list):
        super().__init__(IRawDataset, IPickedDataset, IDumper)
        self.file_list = file_list

    def execute(self, commit):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        pickedDataset = self.pickedDataset.get_pickedDataset(rawDataset)
        self.dumper.insert_value(pickedDataset, commit)

class ComplexDumper:

    def __init__(self, folder_path, db_name):
        self.folder_path = folder_path
        self.db_name = db_name
        
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
            
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 1)

    def execute(self, commit=True):
        r = RawDatasetForComplex(self.folder_path)
        p = PickedDatasetForComplex()
        d = DumperForComplex(self.folder_path, self.db_name)

        for file_list in self.chunked_file_list:
            i = InsertPipelineForComplex(r, p, d, file_list)
            i.execute(commit)