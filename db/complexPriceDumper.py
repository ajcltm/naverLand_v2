from db import Idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, validator
from tqdm import tqdm

class ComplexPriceModel(BaseModel):
    complexNo : str
    ptpNo : str
    hscpNo : str
    date : List[datetime]
    price : List[int]


    @validator('*', pre=True, always=True)
    def deal_with_none(cls, v, values):
        if not v:
            return None
        return v


class ArticleInfoDumper(Idumper.Dumper):

    def get_key_from_fileName(self, fileName):
        hscpNo=fileName.split('.')[0].split('_')[-2]
        ptpNo=fileName.split('.')[0].split('_')[-1]
        return 

    def open_file_get_data(self, file):
        file_path = self.folder_path.joinpath(file)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(file)
        yield {key: data}

    def get_data(self):
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return (list[i:i+c] for i in range(0, len(list), c))
        file_list = os.listdir(self.folder_path)
        # chunked_file_list = chunk_list(file_list, 10)
        return (self.open_file_get_data(file) for file in tqdm(file_list[54995:]))

    def get_value_in_dict(self, dict, key):
        if not dict:
            return None
        return dict.get(key)

    def get_subData(self):
        data = self.get_data()
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
                except:
                    print(articleNo)
        return lst

    def insert_value(self):
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return [list[i:i+c] for i in range(0, len(list), c)]

        data_list = self.get_subData()
        chunked_list = chunk_list(data_list, 10)
        for chunked_data_list in chunked_list:
            value_parts = utils.InsertFormatter().get_values_parts(chunked_data_list)
            sql = f"insert into articleInfo values {value_parts}"
            self.db.cursor().execute(sql)
            self.db.commit()

if __name__ == '__main__':
    ArticleInfoDumper(config.main_path.joinpath('4. articleInfo')).insert_value()
    # import pymysql
    # db = pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='naverland', charset='utf8')
    # sql = 'select * from article'
    # c = db.cursor()
    # c.execute(sql)
    # print(len(list(c.fetchall())))
