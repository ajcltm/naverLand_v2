import pymysql

class CityDumper:

    def __init__(self):
        self.db = pymysql.connect(host='192.168.35.243', port=3306, user='ajcltm', passwd='2642805Ab!', db='naverland', charset='utf8mb4')

    def execute(self):
        sql_1 = "insert into city values ('1100000000', '서울시')"
        sql_2 = "insert into city values ('4100000000', '경기도')"
        print(f'insert city data...')
        self.db.cursor().execute(sql_1)
        self.db.cursor().execute(sql_2)
        self.db.commit()