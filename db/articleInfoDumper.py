from db.idumper import IRawDataset, IPickedDataset, IDumper, IInsertPipeline
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Iterable, List, Dict, Optional
from pydantic import BaseModel, validator, ValidationError
from tqdm import tqdm


class ArticleInfoModel(BaseModel):
    articleNo : str
    articleName : str
    hscpNo : str
    ptpNo : str 
    ptpName : str
    exposeStartYMD : datetime
    exposeEndYMD : datetime
    articleConfirmYMD : datetime
    aptName : str
    aptHouseholdCount : Optional[int]
    aptConstructionCompanyName : Optional[str]
    aptUseApproveYmd : Optional[datetime]
    totalDongCount : Optional[int]
    realestateTypeCode : Optional[str]
    tradeTypeName : Optional[str]
    verificationTypeCode : Optional[str]
    cityName : Optional[str]
    divisionName : Optional[str]
    sectionName : Optional[str]
    householdCountByPtp : Optional[int]
    walkingTimeToNearSubway : Optional[int]
    detailAddress : Optional[str]
    roomCount : Optional[int]
    bathroomCount : Optional[int]
    moveInTypeCode : Optional[str]
    moveInDiscussionPossibleYN : Optional[str]
    monthlyManagementCost : Optional[int]
    monthlyManagementCostIncludeItemName : Optional[str]
    buildingName : Optional[str]
    articleFeatureDescription : Optional[str]
    detailDescription : Optional[str]
    floorLayerName : Optional[str]

    floorInfo : Optional[str]
    priceChangeState : Optional[str]
    dealOrWarrantPrc : Optional[str]
    direction : Optional[str]
    latitude : Optional[float]
    longitude : Optional[float]
    entranceTypeName : Optional[str]

    rentPrice : Optional[int]
    dealPrice : Optional[int]
    warrantPrice : Optional[int]
    allWarrantPrice : Optional[int]
    financePrice : Optional[int]
    premiumPrice : Optional[int]
    isalePrice : Optional[int]
    allRentPrice : Optional[int]
    priceBySpace : Optional[int]
    bondPrice : Optional[int]
    middlePayment : Optional[int]

    realtorName : Optional[str]
    representativeName : Optional[str]
    address : Optional[str]
    representativeTelNo : Optional[str]
    cellPhoneNo : Optional[str]

    supplySpace : Optional[float]
    exclusiveSpace : Optional[float]
    exclusiveRate : Optional[float]

    tagList : Optional[str]

    @validator('*', pre=True, always=True)
    def deal_with_none(cls, v, values):
        if not v:
            return None
        return v

    @validator('roomCount', 'bathroomCount', pre=True)
    def roomCount_validator(cls, v):
        if not v:
            return None
        elif v == '-':
            return None
        else:
            return v
    
    @validator('exposeStartYMD', 'exposeEndYMD', 'articleConfirmYMD', 'aptUseApproveYmd', pre=True, always=True)
    def deal_with_datetime(cls, v, values):
        if not v:
            return v
        elif len(v)==8:
            return datetime.strptime(v, '%Y%m%d')
        elif len(v)==6:
            return datetime.strptime(v, '%Y%m')
        elif len(v)==4:
            return datetime.strptime(v, '%y%m')
    
    @validator('tagList', pre=True, always=True, allow_reuse=True)
    def transform_tagList(cls, v, values):
        if not v:
            return None
        return ', '.join(v)


class RawDatasetForArticleInfo(IRawDataset):

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
        print(f'open files and get raw data :')
        return (self.open_file_and_get_rawData(file) for file in tqdm(file_list))


class PickedDatasetForArticleInfo(IPickedDataset):

    def __init__(self):
        self.error_log = []

    def get_value_in_dict(self, dict, key):
        if not dict:
            return None
        return dict.get(key)

    def get_pickedDataset(self, rawDataset:Iterable[Dict])->Iterable[BaseModel]:
        model_dataset=[]
        for rawData in rawDataset:
            for articleNo, dataDict in rawData.items():             
                ad = dataDict.get('articleDetail')
                aa = dataDict.get('articleAddition')
                af = dataDict.get('articleFacility')
                ap = dataDict.get('articlePrice')
                ar = dataDict.get('articleRealtor')
                asp = dataDict.get('articleSpace')      
                try:
                    model = ArticleInfoModel(
                        articleNo=self.get_value_in_dict(ad, 'articleNo'),
                        articleName=self.get_value_in_dict(ad, 'articleName'),
                        hscpNo=self.get_value_in_dict(ad, 'hscpNo'),
                        ptpNo=self.get_value_in_dict(ad, 'ptpNo'),
                        ptpName=self.get_value_in_dict(ad, 'ptpName'),
                        exposeStartYMD=self.get_value_in_dict(ad, 'exposeStartYMD'),
                        exposeEndYMD=self.get_value_in_dict(ad, 'exposeEndYMD'),
                        articleConfirmYMD=self.get_value_in_dict(ad, 'articleConfirmYMD'),
                        aptName=self.get_value_in_dict(ad, 'aptName'),
                        aptHouseholdCount=self.get_value_in_dict(ad, 'aptHouseholdCount'),
                        aptConstructionCompanyName=self.get_value_in_dict(ad, 'aptConstructionCompanyName'),
                        aptUseApproveYmd=self.get_value_in_dict(ad, 'aptUseApproveYmd'),
                        totalDongCount=self.get_value_in_dict(ad, 'totalDongCount'),
                        realestateTypeCode=self.get_value_in_dict(ad, 'realestateTypeCode'),
                        tradeTypeName=self.get_value_in_dict(ad, 'tradeTypeName'),
                        verificationTypeCode=self.get_value_in_dict(ad, 'verificationTypeCode'),
                        cityName=self.get_value_in_dict(ad, 'cityName'),
                        divisionName=self.get_value_in_dict(ad, 'divisionName'),
                        sectionName=self.get_value_in_dict(ad, 'sectionName'),
                        householdCountByPtp=self.get_value_in_dict(ad, 'householdCountByPtp'),
                        walkingTimeToNearSubway=self.get_value_in_dict(ad, 'walkingTimeToNearSubway'),
                        detailAddress=self.get_value_in_dict(ad, 'detailAddress'),
                        roomCount=self.get_value_in_dict(ad, 'roomCount'),
                        bathroomCount=self.get_value_in_dict(ad, 'bathroomCount'),
                        moveInTypeCode=self.get_value_in_dict(ad, 'moveInTypeCode'),
                        moveInDiscussionPossibleYN=self.get_value_in_dict(ad, 'moveInDiscussionPossibleYN'),
                        monthlyManagementCost=self.get_value_in_dict(ad, 'monthlyManagementCost'),
                        monthlyManagementCostIncludeItemName=self.get_value_in_dict(ad, 'monthlyManagementCostIncludeItemName'),
                        buildingName=self.get_value_in_dict(ad, 'buildingName'),
                        articleFeatureDescription=self.get_value_in_dict(ad, 'articleFeatureDescription'),
                        detailDescription=self.get_value_in_dict(ad, 'detailDescription'),
                        floorLayerName=self.get_value_in_dict(ad, 'floorLayerName'),
                        floorInfo=self.get_value_in_dict(aa, 'floorInfo'),
                        priceChangeState=self.get_value_in_dict(aa, 'priceChangeState'),
                        dealOrWarrantPrc=self.get_value_in_dict(aa, 'dealOrWarrantPrc'),
                        direction=self.get_value_in_dict(aa, 'direction'),
                        latitude=self.get_value_in_dict(aa, 'latitude'),
                        longitude=self.get_value_in_dict(aa, 'longitude'),
                        entranceTypeName=self.get_value_in_dict(af, 'entranceTypeName'),
                        rentPrice=self.get_value_in_dict(ap, 'rentPrice'),
                        dealPrice=self.get_value_in_dict(ap, 'dealPrice'),
                        warrantPrice=self.get_value_in_dict(ap, 'warrantPrice'),
                        allWarrantPrice=self.get_value_in_dict(ap, 'allWarrantPrice'),
                        financePrice=self.get_value_in_dict(ap, 'financePrice'),
                        premiumPrice=self.get_value_in_dict(ap, 'premiumPrice'),
                        isalePrice=self.get_value_in_dict(ap, 'isalePrice'),
                        allRentPrice=self.get_value_in_dict(ap, 'allRentPrice'),
                        priceBySpace=self.get_value_in_dict(ap, 'priceBySpace'),
                        bondPrice=self.get_value_in_dict(ap, 'bondPrice'),
                        middlePayment=self.get_value_in_dict(ap, 'middlePayment'),
                        realtorName=self.get_value_in_dict(ar, 'realtorName'),
                        representativeName=self.get_value_in_dict(ar, 'representativeName'),
                        address=self.get_value_in_dict(ar, 'address'),
                        representativeTelNo=self.get_value_in_dict(ar, 'representativeTelNo'),
                        cellPhoneNo=self.get_value_in_dict(ar, 'cellPhoneNo'),
                        supplySpace=self.get_value_in_dict(asp, 'supplySpace'),
                        exclusiveSpace=self.get_value_in_dict(asp, 'exclusiveSpace'),
                        exclusiveRate=self.get_value_in_dict(asp, 'exclusiveRate'),
                        tagList=self.get_value_in_dict(ad, 'tagList')
                        )
                    model_dataset.append(model)
                except ValidationError as e:
                    self.error_log.append(e.json())        
        return model_dataset

class DumperForArticleInfo(IDumper):

    def insert_value(self, pickedDataset:List[BaseModel], commit:bool)->None:
        value_parts = utils.InsertFormatter().get_values_parts(pickedDataset)
        sql = f"insert into articleInfo values {value_parts}"
        self.db.cursor().execute(sql)
        if commit:
            self.db.commit()


class InsertPipelineForArticleInfo(IInsertPipeline):

    def __init__(self, IRawDataset, IPickedDataset, IDumper, file_list):
        super().__init__(IRawDataset, IPickedDataset, IDumper)
        self.file_list = file_list

    def execute(self, commit):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        pickedDataset = self.pickedDataset.get_pickedDataset(rawDataset)
        print(f'insert data : {pickedDataset[0].articleNo}')
        self.dumper.insert_value(pickedDataset, commit)



class ArticleInfoDumper:

    def __init__(self, folder_path, db_name, start_file_name=None):
        self.folder_path = folder_path
        self.db_name = db_name
        
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
            
        file_list = os.listdir(self.folder_path)
        if start_file_name:
            print(f'articleInfo dumper starts with the file name : {start_file_name}')
            idx = file_list.index(start_file_name)
            file_list = file_list[idx:]
        self.chunked_file_list = chunk_list(file_list, 50)

    def execute(self, commit=True):
        r = RawDatasetForArticleInfo(self.folder_path)
        p = PickedDatasetForArticleInfo()
        d = DumperForArticleInfo(self.folder_path, self.db_name)

        for file_list in self.chunked_file_list:
            i = InsertPipelineForArticleInfo(r, p, d, file_list)
            i.execute(commit)