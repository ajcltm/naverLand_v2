from typing import List, Dict
from webScrap import work, utils, apps
from scrap import config
import requests
import pickle
import os


class WorkingList:

    def get_working_list(self) -> List[work.IWork]:
        folder_path = config.main_path.joinpath('2. complex')
        file_list = os.listdir(folder_path)
        working_list = []
        for file in file_list:
            file_path = folder_path.joinpath(file)
            with open(file_path, 'rb') as fr:
                data = pickle.load(fr)
            working_list += [work.Work(work={'complexNo' : i['complexNo']}) for i in data['complexList']]
        return working_list


class Requester:

    @utils.randomSleep
    def request(self, work:Dict):   
        complexNo = work['complexNo']
        tradeType = 'B1'   #tradeType  A1: 매매, B1: 전세
        url = f'https://new.land.naver.com/api/articles/complex/{complexNo}?realEstateType=APT&tradeType={tradeType}&tag=%3A%3A%3A%3A%3A%3A%3A%3A&rentPriceMin=0&rentPriceMax=900000000&priceMin=0&priceMax=900000000&areaMin=0&areaMax=900000000&oldBuildYears&recentlyBuildYears&minHouseHoldCount&maxHouseHoldCount&showArticle=false&sameAddressGroup=false&minMaintenanceCost&maxMaintenanceCost&priceType=RETAIL&directions=&page=1&complexNo={complexNo}&buildingNos=&areaNos=&type=list&order=rank'
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
        return requests.get(url, headers=headers).json()


class ArticleScraper:

    def execute(self):
        wkng_list = WorkingList()
        save_path = config.main_path.joinpath('3. article')
        fr = Requester()
        fss = apps.FSScraper(IWorkingList=wkng_list, IRequester=fr, save_path=save_path)
        fss.execute()