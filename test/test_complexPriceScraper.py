import unittest
from scrap import complexPriceScraper
from scrap import config
import os
import pickle

# class TestWorkingList(unittest.TestCase):

#     def test_valid_data(self):
#         working_list = complexPriceScraper.WorkingList().get_working_list()
#         print(working_list[:4])


# class TestRequester(unittest.TestCase):

#     def test_valid_complexPrice(self):
#         working_list = complexPriceScraper.WorkingList().get_working_list()
#         data = complexPriceScraper.Requester().request(working_list[0].get_request_key())
#         print(data['realPriceDataYList'])

class TestFSScraper(unittest.TestCase):

    def test_sscraper(self):
        # complexPriceScraper.ComplexPriceScraper().execute()

        save_path = config.main_path.joinpath('5. complexPrice')
        file_list = os.listdir(save_path)
        with open(save_path.joinpath(file_list[0]), mode='rb') as fr:
            data = pickle.load(fr)
        # print(data)
        self.assertEqual(48000, data['realPriceDataYList'][1])

if __name__ == '__main__':
    unittest.main()