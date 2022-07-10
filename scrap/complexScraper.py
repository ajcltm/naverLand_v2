import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))
from webScrap.webScrap import utils

import requests

class FComplexReqeuster:

    def __init__(self, dongCode:str)->None:
        self.dongCode = dongCode

    @utils.randomSleep
    def get_requester(self):
        url = f'https://new.land.naver.com/api/regions/complexes?cortarNo={self.dongCode}&realEstateType=APT&order='
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NDE4MjMwNTEsImV4cCI6MTY0MTgzMzg1MX0.G2LIx6IATbC1JDuGaK10mllYmb061biA6viyofkZiso',
            'Connection': 'keep-alive',

            'Host': 'new.land.naver.com',

            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        }
        return requests.get(url, headers=headers)


def get_working_list(file_path):
    with open(file_path, 'rb') as fr:
        data = pickle.load(fr)
    return [i['cortarNo'] for i in data['regionList']]


if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path.cwd()))
    from webScrap.webScrap import apps
    import pickle
    import os
    from tqdm import tqdm

    dong_path = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '1. dong')
    dong_file_list = os.listdir(dong_path)

    working_list = []
    for dong_file in tqdm(dong_file_list):
        file_path = dong_path.joinpath(dong_file)
        working_list += get_working_list(file_path)
    print('working list length : ',len(working_list))

    mainPath = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '2. complex')
    ss = apps.SScraper(FComplexReqeuster, working_list, mainPath)
    ss.execute()