import unittest
from scrap import articleInfo
from scrap import config
import os
import pickle

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        working_list = articleInfo.WorkingList().get_working_list()
        print(working_list[:4])
        self.assertEqual({'articleNo': '2224155970'}, working_list[0].get_request_key())


class TestRequester(unittest.TestCase):

    def test_valid_articleInfoNo(self):
        working_list = articleInfo.WorkingList().get_working_list()
        data = articleInfo.Requester().request(working_list[0].get_request_key())
        print(data)
        self.assertEqual('개포래미안포레스트 129동', data['articleDetail']['articleName'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        articleInfo.ArticleInfoScraper().execute()

        save_path = config.main_path.joinpath('4. articleInfo')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        self.assertEqual('개포래미안포레스트 129동', data['articleDetail'][0]['articleName'])

if __name__ == '__main__':
    unittest.main()