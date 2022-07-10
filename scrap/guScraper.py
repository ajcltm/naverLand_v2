from typing import List, Dict
from webScrap import work, utils
import requests


class WorkingList:

    def get_working_list(self) -> List[work.IWork]:
        working_list_ = [{'cityNo':'1100000000'}, {'cityNo':'4100000000'}]
        working_list = [work.Work(work=i) for i in working_list_]
        return working_list

class FGuReqeuster:

    def __init__(self, work:Dict)->None:
        self.cityNo = work['cityNo']

    @utils.randomSleep
    def get_request_r(self):
        url = f'https://new.land.naver.com/api/regions/list?cortarNo={self.cityNo}'
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
        return requests.get(url=url, headers=headers)


if __name__ == '__main__':

    
    # mainPath = Path.cwd().joinpath('naverLand_v2', 'scrap', 'db', '0. gu')
    # ss = apps.SScraper(FGuReqeuster, working_list, mainPath)
    # ss.execute()
    ...