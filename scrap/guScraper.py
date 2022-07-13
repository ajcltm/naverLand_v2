from typing import List, Dict
from webScrap import work, utils, apps
from scrap import config
import requests


class WorkingList:

    def get_working_list(self) -> List[work.IWork]:
        working_list_ = [{'cityNo':'1100000000'}, {'cityNo':'4100000000'}]
        working_list = [work.Work(work=i) for i in working_list_]
        return working_list


class Reqeuster:

    @utils.randomSleep
    def request(self, work:Dict):
        cityNo = work['cityNo']
        url = f'https://new.land.naver.com/api/regions/list?cortarNo={cityNo}'
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NDE4MjMwNTEsImV4cCI6MTY0MTgzMzg1MX0.G2LIx6IATbC1JDuGaK10mllYmb061biA6viyofkZiso',
            'Connection': 'keep-alive',
            
            'Host': 'new.land.naver.com',

            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': "Windows",
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        }
        return requests.get(url=url, headers=headers).json()


class GuScraper:

    def execute(self):
        wkng_list = WorkingList()
        save_path = config.main_path.joinpath('0. gu')
        fr = Reqeuster()
        fss = apps.FSScraper(IWorkingList=wkng_list, IRequester=fr, save_path=save_path)
        fss.execute()