import query_data

import unittest
import json

from db_initializer import DBInitializer
from query_join_selector import JoinSelector

class QueryFilterSelectorTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()


    def test_query_1(self):
        with open('../queries/double_query_1.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        join_selector = JoinSelector()
        print "5 " + str(join_selector.eval_query(query_data_obj))

if __name__ == '__main__':
    unittest.main()