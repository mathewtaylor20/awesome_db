from __future__ import absolute_import

import unittest
import json
from db_initializer import DBInitializer
from query_table_selector import TableSelector


class TestQuerySingleValue(unittest.TestCase):

    def setUp(self):
        db_engine = DBInitializer()
        db_engine.initalize_db()

    def test_query_1(self):
        with open('../queries/double_query_1.json') as json_data:
            query = json.loads(json_data.read())
        db_optimizer = TableSelector()
        print "1 " + str(db_optimizer.eval_query(query))


    def test_query_2(self):
        with open('../queries/double_query_2.json') as json_data:
            query = json.loads(json_data.read())
        db_optimizer = TableSelector()
        print "2 " + str(db_optimizer.eval_query(query))


    def test_query_3(self):
        with open('../queries/double_query_3.json') as json_data:
            query = json.loads(json_data.read())
        db_optimizer = TableSelector()
        print "3 " + str(db_optimizer.eval_query(query))

    def test_query_4(self):
        with open('../queries/double_query_4.json') as json_data:
            query = json.loads(json_data.read())
        db_optimizer = TableSelector()
        print "4 " + str(db_optimizer.eval_query(query))

    def test_query_5(self):
        with open('../queries/double_query_1.json') as json_data:
            query = json.loads(json_data.read())
        db_optimizer = TableSelector()
        print "5 " + str(db_optimizer.eval_query(query))


if __name__ == '__main__':
    unittest.main()