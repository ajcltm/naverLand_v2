import unittest
from db import dongDumper
from pathlib import Path

class Test_Dongdumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '1. dong')
        dongDumper.DongDumper(folder_path, 'naverland').execute(commit=False)

if __name__ == '__main__':
    unittest.main()