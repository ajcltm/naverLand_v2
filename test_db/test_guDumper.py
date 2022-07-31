import unittest
from db import guDumper
from pathlib import Path

class Test_Gudumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '0. gu')
        guDumper.GuDumper(folder_path, 'naverland').execute(commit=False)

if __name__ == '__main__':
    unittest.main()