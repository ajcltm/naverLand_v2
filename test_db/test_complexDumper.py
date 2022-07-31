import unittest
from db import complexDumper
from pathlib import Path

class Test_Dongdumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '2. complex')
        complexDumper.ComplexDumper(folder_path, 'naverland').execute(commit=False)

if __name__ == '__main__':
    unittest.main()