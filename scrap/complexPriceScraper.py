from typing import List, Dict
from webScrap import work, utils, apps
from scrap import config
import requests
import pickle
import os
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor 
from concurrent.futures import as_completed

def process_articleinfo_file(file):
    try:
        file_path = config.main_path.joinpath('4. articleInfo', file)
        with open(file_path, 'rb') as fr:
            data = pickle.load(fr)
        return dict(hscpNo=data['articleDetail']['hscpNo'], ptpNo=data['articleDetail']['ptpNo'])
    except:
        print(file)
        return None

class WorkingList:

    def get_working_list(self) -> List[work.IWork]:
        folder_path = config.main_path.joinpath('4. articleInfo')
        file_list = os.listdir(folder_path)
        working_list = []
        executor = ThreadPoolExecutor(max_workers=5)
        futures  = [executor.submit(process_articleinfo_file, file) for file in tqdm(file_list)]
        for future in tqdm(as_completed(futures)):
            r = future.result()
            if r is not None:
                working_list.append(r)
        df = pd.DataFrame(working_list)
        lst = df[['hscpNo', 'ptpNo']].drop_duplicates().dropna(how="all").to_dict(orient='records')
        return [work.Work(work=i) for i in lst]


class Requester:

    @utils.randomSleep
    def request(self, work:Dict):
        complexNo = work['hscpNo']
        ptpNo = work['ptpNo']
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
        return requests.get(url, headers=headers).json()


class ComplexPriceScraper:

    def execute(self):
        wkng_list = WorkingList()
        save_path = config.main_path.joinpath('5. complexPrice')
        fr = Requester()
        fss = apps.FSScraper(IWorkingList=wkng_list, IRequester=fr, save_path=save_path)
        fss.execute()
