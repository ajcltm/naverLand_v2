import unittest
from data import returnDataset
import pandas as pd

class Test_returnDataset(unittest.TestCase):

    def test_sql_query(self):
        c = returnDataset.Return().query.get_return()
        print(pd.DataFrame([i.dict() for i in c]))


if __name__ == '__main__':

    unittest.main()