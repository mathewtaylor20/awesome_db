from __future__ import absolute_import
from timeit import default_timer as timer

import unittest
import json
import query_data

from db_initializer import DBInitializer
from query_executor import QueryExecutor
from query_planner import QueryPlanner


class AwesomeDBTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()

    def test_query_1(self):

        with open('../queries/4.json') as json_data:
            query = json.loads(json_data.read())
        start = timer()
        query_data_obj = query_data.create_query_data(query)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        print '\nPlan : ' + str(query_plan)
        query_executor = QueryExecutor()
        query_executor.execute_query(query_data_obj, query_plan)
        end = timer()
        print('\n\nQuery run time : ' + str(end - start))


if __name__ == '__main__':
    unittest.main()