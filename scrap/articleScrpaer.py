import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from webScrap.webScrap import utils

import requests

class FArticleReqeuster:

    def __init__(self, complexCode:str, tradeType='A1')->None:
        self.complexCode = complexCode
        self.tradeType = tradeType

    @utils.randomSleep
    def get_requester(self):
        url = f'https://new.land.naver.com/api/articles/complex/{self.complexCode}?realEstateType=APT&tradeType={self.tradeType}&tag=%3A%3A%3A%3A%3A%3A%3A%3A&rentPriceMin=0&rentPriceMax=900000000&priceMin=0&priceMax=900000000&areaMin=0&areaMax=900000000&oldBuildYears&recentlyBuildYears&minHouseHoldCount&maxHouseHoldCount&showArticle=false&sameAddressGroup=false&minMaintenanceCost&maxMaintenanceCost&priceType=RETAIL&directions=&page=1&complexNo={self.complexCode}&buildingNos=&areaNos=&type=list&order=rank'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NTQxODI5NDIsImV4cCI6MTY1NDE5Mzc0Mn0.qH5BpVq1KtIcdeNWa1c5rxe6OA3y1q6s7HfB6Zowkbo',
            'Connection': 'keep-alive',

            'Host': 'new.land.naver.com',
            'Referer': 'https://new.land.naver.com/complexes/137413?ms=37.5038337,127.0508535,16&a=APT&b=A1&e=RETAIL',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
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
    return [i['complexNo'] for i in data['complexList']]


if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path.cwd()))
    from webScrap.webScrap import apps
    import pickle
    import os
    from tqdm import tqdm

    complex_path = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '2. complex')
    complex_file_list = os.listdir(complex_path)

    working_list = []
    for complex_file in tqdm(complex_file_list):
        file_path = complex_path.joinpath(complex_file)
        working_list += get_working_list(file_path)
    print('working list length : ',len(working_list))
    
    mainPath = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '3. article')   
    ss = apps.SScraper(FArticleReqeuster, working_list, mainPath)
    ss.execute()

    # r = FArticleReqeuster('137413').get_requester()
    # print(r.json())