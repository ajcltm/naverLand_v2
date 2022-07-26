from ast import Return
from sqlite3 import adapt
from click import open_file
from db import idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator, ValidationError
from tqdm import tqdm
from pathlib import Path

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

class ArticleInfoDumper(idumper.Dumper):

    def __init__(self, file_path):
        super().__init__(file_path)
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
        file_list = os.listdir(self.folder_path)
        self.chunked_file_list = chunk_list(file_list, 50)

    def get_key_from_fileName(self, fileName):
        return fileName.split('.')[0].split('_')[-1]

    def open_file_get_data(self, file):
        file_path = self.folder_path.joinpath(file)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(file)
        yield {key: data}

    def get_data(self, file_list):
        return (self.open_file_get_data(file) for file in tqdm(file_list))

    def get_value_in_dict(self, dict, key):
        if not dict:
            return None
        return dict.get(key)

    def get_subData(self, file_list):
        data = self.get_data(file_list)
        lst = []
        for article_dict in data:
            for articleNo, article_data in list(article_dict)[0].items():
                ad = article_data.get('articleDetail')
                aa = article_data.get('articleAddition')
                af = article_data.get('articleFacility')
                ap = article_data.get('articlePrice')
                ar = article_data.get('articleRealtor')
                asp = article_data.get('articleSpace')
                try:
                    lst.append(ArticleInfoModel(
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
                        ))
                except ValidationError as e:
                    print(f'validationError {articleNo}')
                    print(e.json())
        return lst

    def insert_value(self):
        # self.db.query('set GLOBAL max_allowed_packet=67108864')
        # self.db.query('SET GLOBAL connect_timeout=6000')
        # self.db.commit()
        
        for file_list in self.chunked_file_list:
            data_list = self.get_subData(file_list)
            value_parts = utils.InsertFormatter().get_values_parts(data_list)
            sql = f"insert into articleInfo values {value_parts}"
            path = Path().home().joinpath('Desktop', 'sql.txt')
            with open(path, mode='w', encoding='utf8') as fw:
                fw.write(sql)
            self.db.cursor().execute(sql)
            self.db.commit()

if __name__ == '__main__':
    ArticleInfoDumper(config.main_path.joinpath('4. articleInfo')).insert_value()
    # import pymysql
    # db = pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='naverland', charset='utf8')
    # sql = 'select count(*) from articleInfo'
    # c = db.cursor()
    # c.execute(sql)
    # print(list(c.fetchall()))
    # file_list = ['articleNo_2221305459.pickle']
    # ArticleInfoDumper(config.main_path.joinpath('4. articleInfo')).get_subData(file_list)
    # gen = ArticleInfoDumper(config.main_path.joinpath('4. articleInfo')).get_data(file_list)
