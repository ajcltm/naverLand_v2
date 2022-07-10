from asyncio import as_completed
from pickle import FALSE
import sys
from pathlib import Path

from pkg_resources import working_set
sys.path.append(str(Path.cwd()))
from webScrap.webScrap import utils

import requests
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

class FComplexPriceReqeuster:

    def __init__(self, i)->None:
        self.complexNo = i['complexNo']
        self.ptpNo = i['ptpNo']

    @utils.randomSleep
    def get_requester(self):
        url = f'https://new.land.naver.com/api/complexes/{complexNo}/prices?complexNo={complexNo}&year=5&tradeType=A1&areaNo={ptpNo}&type=chart'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NDI0MzM3MDAsImV4cCI6MTY0MjQ0NDUwMH0.0E_bLezMEZWh-H_YEXWAU3gwjpUiyc-NPS_9Dbx_BRw',
            'Connection': 'keep-alive',
            'Host': 'new.land.naver.com',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': "Windows",
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
        }
        return requests.get(url, headers=headers)

def get_working_list(file_path):
    with open(file_path, 'rb') as fr:
        data = pickle.load(fr)
    return dict(hscpNo=data['articleDetail']['hscpNo'], ptpNo=data['articleDetail']['ptpNo'])
    
def process_articleinfo_file(articleInfo_file):
    try:
        file_path = ariticleInfo_path.joinpath(articleInfo_file)
        return get_working_list(file_path)
    except:
        print(articleInfo_file)
        return None

if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path.cwd()))
    from webScrap.webScrap import apps
    import pandas as pd
    import pickle
    import os
    from tqdm import tqdm

    ariticleInfo_path = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '4. articleInfo')
    articleInfo_file_list = os.listdir(ariticleInfo_path)

    if False:
        working_list = []
        for articleInfo_file in tqdm(articleInfo_file_list):
            file_path = ariticleInfo_path.joinpath(articleInfo_file)
            try:
                working_list.append(get_working_list(file_path))  
            except:
                print(articleInfo_file)
    elif False:
        executor = ThreadPoolExecutor()
        working_list = []
        temp_list = executor.map(process_articleinfo_file, articleInfo_file_list)
        for temp in tqdm(temp_list):
            working_list.append(temp)
    else:
        working_list = []
        executor = ThreadPoolExecutor()
        futures  = [executor.submit(process_articleinfo_file, articleInfo_file) for articleInfo_file in tqdm(articleInfo_file_list)]
        print('futures len : ', len(futures))
        for future in tqdm(as_completed(futures)):
            r = future.result()
            if r is not None:
                working_list.append(r)

    print('working list length : ',len(working_list))
    df = pd.DataFrame(working_list)
    lst = df[['hscpNo', 'ptpNo']].drop_duplicates().dropna(how="all").to_dict(orient='records')
    print(lst[:10])
    mainPath = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '5. complexPrice')   
    ss = apps.SScraper(FComplexPriceReqeuster, working_list, mainPath)
    ss.execute()

