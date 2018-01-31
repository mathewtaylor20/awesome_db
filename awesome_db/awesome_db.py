import json
from timeit import default_timer as timer

from db_initializer import DBInitializer
from query_data import QueryData
from query_executor import QueryExecutor
from query_planner import QueryPlanner

class AwesomeDB:

    def run_awesome(self):
        db_initializer = DBInitializer()
        self.run_db(db_initializer)


    def run_db(self, db_initializer):
        quit = False
        initializing = False
        initialized = False
        initialization_step = 0

        indexing = False
        split_file = False
        stats = False
        optimization = False
        print_json = False

        while quit == False or initialized == False:
            if initialization_step == 0 or initialized:
                command = raw_input("awesome_db: ")

            if command == "quit" or command == "q":
                print "command q"
                break

            elif initializing and not initialized:
                if initialization_step == 1:
                    command = raw_input("awesome_db - Do you want to use indexing? : ")
                    if command == "yes" or command == "y" or command == "":
                        split_file = True
                        indexing = True
                    else:
                        split_file = False
                        indexing = False
                    initialization_step = 2

                elif initialization_step == 2:
                    command = raw_input("awesome_db - Do you want to use stats? : ")
                    if command == "yes" or command == "y" or command == "":
                        stats = True
                    else:
                        stats = False
                    initialization_step = 3

                elif initialization_step == 3:
                    command = raw_input("awesome_db - Do you want to use optimization? : ")
                    if command == "yes" or command == "y":
                        optimization = True
                    else:
                        optimization = False
                    initialization_step = 4

                elif initialization_step == 4:
                    command = raw_input("awesome_db - Do you want to print json? : ")
                    if command == "yes" or command == "y":
                        print_json = True
                    else:
                        print_json = False
                    initialization_step = 5

                elif initialization_step == 5:
                    print "Initializing db"
                    initialized = db_initializer.initalize_db(split_file, indexing, stats)
                    initializing = False

            elif command == "initialize" or command == "i":
                initializing = True
                initialized = False
                initialization_step = 1

            elif initialized and not initializing:
                if command == "execute" or command == "e":
                    command = raw_input("awesome_db - enter query file : ")
                    query_file_path = "../queries/" + command
                    self.run_query(query_file_path, indexing, stats, optimization, print_json)

                elif command == "output" or command == "o":
                    command = raw_input("awesome_db - enter query file : ")
                    query_file_path = "../queries/" + command
                    command = raw_input("awesome_db - enter output file path : ")
                    output_file_path = command
                    self.output_query(query_file_path, output_file_path, optimization, print_json)

                elif command == "plan" or command == "p":
                    command = raw_input("awesome_db - enter query file path : ")
                    query_file_path = "../queries/" + command
                    self.plan_query(query_file_path)



            else:
                print "Unknown command"

        print "Exiting - goodbye!!!"


    def run_query(self, query_file_path, indexing, stats, optimization, print_json):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data = QueryData()
        query_data_obj = query_data.create_query_data(query, indexing, stats, optimization)
        if print_json:
            print "\nQuery : " + str(query_data_obj)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        if print_json:
            print "\nPlan : " + str(query_plan)
        query_executor = QueryExecutor()
        query_result = query_executor.execute_query(query_data_obj, query_plan, indexing)
        if print_json:
            print "\nResult : \n" + str(query_result)
        end = timer()
        print("\n\nQuery run time : " + str(end - start))


    def output_query(self, query_file_path, indexing, stats, optimization, print_json):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data = QueryData()
        query_data_obj = query_data.create_query_data(query, indexing, stats, optimization)
        if print_json:
            print "\nQuery : " + str(query_data_obj)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        if print_json:
            print "\nPlan : " + str(query_plan)
        query_executor = QueryExecutor()
        query_result = query_executor.execute_query(query_data_obj, query_plan)
        end = timer()
        print(str(len(query_result)) + " results returned in " + str(end - start))
        if print_json:
            print "\nResult : \n" + str(query_result)
        print("\n\nQuery run time : " + str(end - start))


    def plan_query(self, query_file_path, indexing, stats, optimization, print_json):
        start = timer()
        with open(query_file_path) as json_data:
            query = json.loads(json_data.read())
        query_data = QueryData()
        query_data_obj = query_data.create_query_data(query, indexing, stats, optimization)
        query_planner = QueryPlanner()
        query_plan = query_planner.eval_query(query_data_obj)
        print "\nPlan : " + str(query_plan)
        end = timer()
        print("\n\nPlan run time : " + str(end - start))


def main():
    awesome_db = AwesomeDB()
    awesome_db.run_awesome()

if __name__ == "__main__":
    main()