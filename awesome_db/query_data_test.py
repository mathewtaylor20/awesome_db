from __future__ import absolute_import

import unittest
import json

from query_data import QueryData
from db_initializer import DBInitializer


class QueryDataTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()


    def test_query_data_1(self):
        with open('../queries/1.json') as json_data:
            query = json.loads(json_data.read())
        query_data = QueryData()
        query_data_obj = query_data.create_query_data(query)

if __name__ == '__main__':
    unittest.main()