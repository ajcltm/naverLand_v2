import unittest
from scrap import guScraper

class TestWorkingList(unittest.TestCase):

    def test_valid_data(self):
        request_key = [{'cityNo':'1100000000'}, {'cityNo':'4100000000'}]
        working_list = guScraper.WorkingList().get_working_list()

        self.assertEqual(request_key[0], working_list[0].get_request_key())
        self.assertEqual(request_key[1], working_list[1].get_request_key())

class TestRequester(unittest.TestCase):

    def test_valid_cityNo(self):
        working_list = guScraper.WorkingList().get_working_list()
        r = guScraper.FGuReqeuster().get_request_r(working_list[0].get_request_key())
        self.assertEqual('강남구', r.json()['regionList'][0]['cortarName'])

class TestSScraper(unittest.TestCase):

    def test_sscraper(self):
        guScraper.GuScraper().execute()



if __name__ == '__main__':
    unittest.main()