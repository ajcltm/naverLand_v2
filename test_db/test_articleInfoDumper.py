import unittest
from db import articleInfoDumper
from pathlib import Path

class Test_Dongdumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '4. articleInfo')
        articleInfoDumper.ArticleInfoDumper(folder_path, 'naverland').execute()

if __name__ == '__main__':
    unittest.main()