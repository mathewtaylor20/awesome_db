from __future__ import absolute_import

import unittest
import json
import query_data

from db_initializer import DBInitializer
from query_table_selector import TableSelector


class QueryTableSelectorTest(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()

    def test_query_5(self):
        with open('../queries/double_query_1.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        table_selector = TableSelector()
        print "5 " + str(table_selector.eval_query(query_data_obj))

'''
    def test_query_1(self):
        with open('../queries/double_query_1.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        db_optimizer = TableSelector()
        results_list = []
        print "1 " + str(db_optimizer.eval_query(query_data_obj, results_list))


    def test_query_2(self):
        with open('../queries/double_query_2.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        db_optimizer = TableSelector()
        results_list = []
        print "2 " + str(db_optimizer.eval_query(query_data_obj, results_list))


    def test_query_3(self):
        with open('../queries/double_query_3.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        db_optimizer = TableSelector()
        results_list = []
        print "3 " + str(db_optimizer.eval_query(query_data_obj, results_list))

    def test_query_4(self):
        with open('../queries/double_query_4.json') as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        db_optimizer = TableSelector()
        results_list = []
        print "4 " + str(db_optimizer.eval_query(query_data_obj, results_list))
'''

if __name__ == '__main__':
    unittest.main()
