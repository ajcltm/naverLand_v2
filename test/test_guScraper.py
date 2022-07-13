import unittest
from scrap import guScraper
from scrap import config
import os
import pickle

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        request_key = [{'cityNo':'1100000000'}, {'cityNo':'4100000000'}]
        working_list = guScraper.WorkingList().get_working_list()

        self.assertEqual(request_key[0], working_list[0].get_request_key())
        self.assertEqual(request_key[1], working_list[1].get_request_key())

class TestRequester(unittest.TestCase):

    def test_valid_cityNo(self):
        working_list = guScraper.WorkingList().get_working_list()
        data = guScraper.Reqeuster().request(working_list[0].get_request_key())
        self.assertEqual('강남구', data['regionList'][0]['cortarName'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        guScraper.GuScraper().execute()

        save_path = config.main_path.joinpath('0. gu')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        self.assertEqual('강남구', data['regionList'][0]['cortarName'])




if __name__ == '__main__':
    unittest.main()