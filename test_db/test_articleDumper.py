import unittest
from db import articleDumper
from pathlib import Path

class Test_Dongdumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '3. article')
        articleDumper.ArticleDumper(folder_path, 'naverland').execute(commit=False)

if __name__ == '__main__':
    unittest.main()