from __future__ import absolute_import

import unittest
import json
import query_data

from db_initializer import DBInitializer
from query_executor import QueryExecutor


class QueryExecutorTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()

    def test_plan_hash(self):
        print '\n\n\n Running Hash plan test'
        with open('../queries/1.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)

        with open('../plans/double_plan_1_hash.json') as json_data:
            query_plan = json.loads(json_data.read())

        query_executor = QueryExecutor()
        query_executor.execute_query(query_data_obj, query_plan)


    def test_plan_loop(self):
        print '\n\n\n Running Loop plan test'
        with open('../queries/1.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)

        with open('../plans/double_plan_3_loop.json') as json_data:
            query_plan = json.loads(json_data.read())

        query_executor = QueryExecutor()
        query_executor.execute_query(query_data_obj, query_plan)


    def test_plan_merge(self):
        print '\n\n\n Running Merge plan test'
        with open('../queries/3.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)

        with open('../plans/double_plan_3_merge.json') as json_data:
            query_plan = json.loads(json_data.read())

        query_executor = QueryExecutor()
        query_executor.execute_query(query_data_obj, query_plan)

if __name__ == '__main__':
    unittest.main()