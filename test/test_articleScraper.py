import unittest
from scrap import articleScrpaer
from scrap import config
import os
import pickle

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        working_list = articleScrpaer.WorkingList().get_working_list()
        print(working_list[:4])
        self.assertEqual({'complexNo': '146479'}, working_list[0].get_request_key())


class TestRequester(unittest.TestCase):

    def test_valid_complexNo(self):
        working_list = articleScrpaer.WorkingList().get_working_list()
        data = articleScrpaer.Requester().request(working_list[1].get_request_key())
        print(data)
        self.assertEqual('개포래미안포레스트', data['articleList'][0]['articleName'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        articleScrpaer.ArticleScraper().execute()

        save_path = config.main_path.joinpath('3. article')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        self.assertEqual('개포래미안포레스트', data['articleList'][0]['articleName'])

if __name__ == '__main__':
    unittest.main()