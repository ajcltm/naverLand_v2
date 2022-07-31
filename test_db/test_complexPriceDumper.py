import unittest
from db import complexPriceDumper
from pathlib import Path

class Test_ComplexPriceDumper(unittest.TestCase):

    def test_nomal(self):
        folder_path = Path('F:').joinpath('data', 'naverLand', '220723', '5. complexPrice')
        complexPriceDumper.ComplexPriceDumper(folder_path, 'naverland').execute()

if __name__ == '__main__':
    unittest.main()