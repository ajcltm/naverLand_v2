from db import idumper
from scrap import config
from db import utils
import os
import _pickle as pickle
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator

class ArticleModel(BaseModel):
    articleNo:str
    articleName:str
    complexNo:str
    

class ArticleDumper(idumper.Dumper):

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
        for complex_dict in data:
            for complex_no, complex_data in complex_dict.items():
                for article_data in complex_data['articleList']:
                    lst.append(ArticleModel(
                        articleNo=article_data.get('articleNo'), 
                        articleName=article_data.get('articleName'), 
                        complexNo=complex_no
                        ))
        return lst

    def insert_value(self):
        def chunk_list(list, n):
            c, r = divmod(len(list), n)
            return [list[i:i+c] for i in range(0, len(list), c)]

        data_list = self.get_subData()
        chunked_list = chunk_list(data_list, 4)
        for chunked_data_list in chunked_list:
            value_parts = utils.InsertFormatter().get_values_parts(chunked_data_list)
            sql = f"insert into article values {value_parts}"
            self.db.cursor().execute(sql)
            self.db.commit()

if __name__ == '__main__':
    ArticleDumper(config.main_path.joinpath('3. article')).insert_value()
    # import pymysql
    # db = pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='naverland', charset='utf8')
    # sql = 'select * from article'
    # c = db.cursor()
    # c.execute(sql)
    # print(len(list(c.fetchall())))
