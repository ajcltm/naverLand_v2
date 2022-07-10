# import sys
# from pathlib import Path
# sys.path.append(str(Path.cwd()))
from webScrap.webScrap import utils

import requests

class FArticleReqeuster:

    def __init__(self, articleNo:str)->None:
        self.articleNo = articleNo

    @utils.randomSleep
    def get_requester(self):
        url = f'https://new.land.naver.com/api/articles/{self.articleNo}?complexNo='
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NDE4MjMwNTEsImV4cCI6MTY0MTgzMzg1MX0.G2LIx6IATbC1JDuGaK10mllYmb061biA6viyofkZiso',
            'Connection': 'keep-alive',
            'Cookie': 'NNB=XDAEADAWTN2V6; NRTK=ag#all_gr#1_ma#-2_si#0_en#0_sp#0; ASID=d3cfd3330000017b6b53b7920000006c; nx_ssl=2; _ga=GA1.2.1641582590.1602312031; _ga_7VKFYR6RV1=GS1.1.1640622961.64.0.1640622961.60; nhn.realestate.article.rlet_type_cd=A01; nhn.realestate.article.ipaddress_city=1100000000; landHomeFlashUseYn=N; nhn.realestate.article.trade_type_cd=A1; realestate.beta.lastclick.cortar=1168010300; REALESTATE=Tue%20Jan%2011%202022%2000%3A10%3A43%20GMT%2B0900%20(KST); wcs_bt=4f99b5681ce60:1641827444',
            'Host': 'new.land.naver.com',
            'Referer': 'https://new.land.naver.com/complexes/8928?ms=37.496437,127.07371950000001,17&a=APT&b=A1:B1&e=RETAIL',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': "Windows",
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        }
        return requests.get(url, headers=headers)


def get_working_list(file_path):
    with open(file_path, 'rb') as fr:
        data = pickle.load(fr)
    return [i['articleNo'] for i in data['articleList']]


if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path.cwd()))
    from webScrap.webScrap import apps
    import pickle
    import os
    from tqdm import tqdm

    ariticle_path = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '3. article')
    article_file_list = os.listdir(ariticle_path)

    # working_list = []
    # for article_file in tqdm(article_file_list):
    #     file_path = ariticle_path.joinpath(article_file)
    #     working_list += get_working_list(file_path)
    # print('working list length : ',len(working_list))
    
    # mainPath = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '4. articleInfo')   
    # ss = apps.SScraper(FArticleReqeuster, working_list, mainPath)
    # ss.execute()
