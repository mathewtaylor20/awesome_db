from __future__ import absolute_import

import unittest
import json
import query_data

from db_initializer import DBInitializer
from query_planner import QueryPlanner


class QueryTableSelectorTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()

    def test_query_1(self):
        with open('../queries/double_query_3.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        query_planner = QueryPlanner()
        print '\n\n query_plan : ' + str(query_planner.eval_query(query_data_obj))


if __name__ == '__main__':
    unittest.main()