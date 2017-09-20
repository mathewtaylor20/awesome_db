from timeit import default_timer as timer

import json
import query_data

from db_initializer import DBInitializer
from query_executor import QueryExecutor
from query_planner import QueryPlanner

class AwesomeDB:

    def run_awesome(self):
        db_initializer = DBInitializer()
        self.run_db(db_initializer)


    def run_db(self, db_initializer):
        quit = False
        initialized = False
        initialization_step = 0

        query_planning = False
        indexing = False
        split_file = False

        while quit == False or initialized == False:
            if initialization_step == 0 or initialized:
                command = raw_input('awesome_db: ')

            if command == 'quit' or command == 'q':
                print 'command q'
                break

            elif not initialized:
                if command == 'initialize' or command == 'i':
                    initialization_step = 1
                    command = raw_input('awesome_db - Do you want to split your db file? : ')

                elif initialization_step == 1:
                    if command == 'yes' or command == 'y':
                        split_file = True
                    initialization_step = 2
                    command = raw_input('awesome_db - Do you want to use indexing? : ')

                elif initialization_step == 2:
                    if command == 'yes' or command == 'y':
                        indexing = True
                    initialization_step = 3
                    command = raw_input('awesome_db - Do you want to use table stats? : ')

                elif initialization_step == 3:
                    if command == 'yes' or command == 'y':
                        stats = True
                    initialization_step = 4
                    command = raw_input('awesome_db - Do you want to use query planning? : ')

                elif initialization_step == 4:
                    if command == 'yes' or command == 'y':
                        query_planning = True
                    initialization_step = 5
                    command = raw_input('awesome_db - Do you want to use other optimization? : ')

                elif initialization_step == 4:
                    print 'Initializing db'
                    initialized = db_initializer.initalize_db(split_file, indexing, stats)

            elif initialized:
                if command == 'execute' or command == 'e':
                    command = raw_input('awesome_db - enter query file path : ')
                    query_file_path = command
                    self.run_query(query_file_path)

                elif command == 'output' or command == 'o':
                    command = raw_input('awesome_db - enter query file path : ')
                    query_file_path = command
                    command = raw_input('awesome_db - enter output file path : ')
                    output_file_path = command
                    self.output_query(query_file_path, output_file_path)

                elif command == 'plan' or command == 'p':
                    command = raw_input('awesome_db - enter query file path : ')
                    query_file_path = command
                    self.plan_query(query_file_path)

            else:
                print 'Unknown command'

        print 'Exiting - goodbye!!!'


    def run_query(self, query_file_path, split_file, indexing, stats, query_planning):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        print '\nPlan : ' + str(query_plan)
        query_executor = QueryExecutor()
        query_result = query_executor.execute_query(query_data_obj, query_plan)
        print '\nResult : ' + str(query_result)
        end = timer()
        print('\n\nQuery run time : ' + str(end - start))


    def output_query(self, query_file_path):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        print '\nPlan : ' + str(query_plan)
        query_executor = QueryExecutor()
        query_result = query_executor.execute_query(query_data_obj, query_plan)
        end = timer()
        print(str(len(query_result)) + ' results returned in ' + str(end - start))
        print '\nResult : \n' + str(query_result)
        print('\n\nQuery run time : ' + str(end - start))


    def plan_query(self, query_file_path):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data_obj = query_data.create_query_data(query)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        print '\nPlan : ' + str(query_plan)
        end = timer()
        print('\n\nPlan run time : ' + str(end - start))


def main():
    awesome_db = AwesomeDB()
    awesome_db.run_awesome()

if __name__ == "__main__":
    main()