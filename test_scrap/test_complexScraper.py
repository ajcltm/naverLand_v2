import unittest
from scrap import complexScraper
from scrap import config
import os
import pickle

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        working_list = complexScraper.WorkingList().get_working_list()
        print(working_list[:4])
        self.assertEqual({'dongNo': '1168010300'}, working_list[0].get_request_key())


class TestRequester(unittest.TestCase):

    def test_valid_complexNo(self):
        working_list = complexScraper.WorkingList().get_working_list()
        data = complexScraper.Reqeuster().request(working_list[0].get_request_key())
        print(data)
        self.assertEqual('YH빌리지', data['complexList'][0]['complexName'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        # articleScrpaer.ArticleScraper().execute()

        save_path = config.main_path.joinpath('2. complex')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        self.assertEqual('YH빌리지', data['complexList'][0]['complexName'])

if __name__ == '__main__':
    unittest.main()