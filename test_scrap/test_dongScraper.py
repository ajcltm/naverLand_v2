import unittest
from scrap import dongScraper
from scrap import config
import os
import pickle

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        working_list = dongScraper.WorkingList().get_working_list()
        print(working_list[:4])
        self.assertEqual({'guNo': '1168000000'}, working_list[0].get_request_key())
        self.assertEqual({'guNo': '4159000000'}, working_list[-1].get_request_key())

class TestRequester(unittest.TestCase):

    def test_valid_guNo(self):
        working_list = dongScraper.WorkingList().get_working_list()
        data = dongScraper.Reqeuster().request(working_list[0].get_request_key())
        print(data)
        self.assertEqual('개포동', data['regionList'][0]['cortarName'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        dongScraper.DongScraper().execute()

        save_path = config.main_path.joinpath('1. dong')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        self.assertEqual('개포동', data['regionList'][0]['cortarName'])

if __name__ == '__main__':
    unittest.main()