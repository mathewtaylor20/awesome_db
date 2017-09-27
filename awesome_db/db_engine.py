import csv
import fileinput
import sys
from os import listdir
from timeit import default_timer as timer

import table_data


class DBEngine:

    def initialize_db(self):
        quit = False
        initialized = False
        initialization_step = 0
        while quit == False or initialized == True:
            if initialization_step == 0:
                command = raw_input('awesome_db: ')

            if command == 'quit' or command == 'q':
                print 'command q'
                quit = True

            elif command == 'initialize' or command == 'i':
                command = raw_input('awesome_db - Do you want to split your db file? : ')
                initialization_step = 1

            elif initialization_step == 1:
                if command == 'yes' or command == 'y':
                    split_file = True
                else:
                    split_file = False
                initialization_step = 2
                command = raw_input('awesome_db - Do you want to use indexing? : ')

            elif initialization_step == 2:
                if command == 'yes' or command == 'y':
                    indexing = True
                else:
                    indexing = False
                initialization_step = 3
                command = raw_input('awesome_db - Do you want to use query planning? : ')

            elif initialization_step == 3:
                if command == 'yes' or command == 'y':
                    query_planning = True
                else:
                    query_planning = False
                initialization_step = 4
                command = raw_input('awesome_db - Do you want to use other optimization? : ')

            elif initialization_step == 4:
                initialized = True
                print 'Initializing db - please wait...'

            else:
                print 'Unknown command'
        if quit:
            print 'Exiting - goodbye!!!'
            return

    def run_db(self):
        while True:

            if command == 'quit' or command == 'q':
                print 'command q'
                break

            elif command == 'execute' or command == 'e':
                command = raw_input('awesome_db - enter query file path : ')
                input_file_path = command

            elif command == 'output' or command == 'o':
                command = raw_input('awesome_db - enter query file path : ')
                input_file_path = command
                command = raw_input('awesome_db - enter output file path : ')
                output_file_path = command

            elif command == 'plan' or command == 'p':
                command = raw_input('awesome_db - enter query file path : ')
                input_file_path = command

            else:
                print 'Unknown command'
        print 'Exiting - goodbye!!!'




    def get_tables(self):
        for file in listdir('../data/'):
            self.desc_table(file)


    def desc_table(self, tableName):
        with open('../data/' + tableName, 'rb') as csvfile:
            table_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in table_reader:
                print(row)


    def check_table(self, table_name):
        if table_name + '.csv' in listdir('../data/'):
            return True
        print 'Table does not exist'
        return False


    def check_params(self, table_name, params):
        i = 0
        for column_type in table_data.table_data[table_name]['column_types']:
            if not isinstance(column_type(params[i]), column_type):
                print 'Incorrect param ' + str(params[i]) + ' (' + str(type(params[i])) + ')' + ' is not type ' + str(column_type)
                return False
            i = i + 1
        return True


    def check_sum_params(self, table_name, params):
        i = 0
        for param in params:
            if param == '*' and table_data.table_data[table_name]['column_types'][i] != int:
                print 'Incorrect sum on column type ' + str(table_data.table_data[table_name]['column_types'][i])
                return False
            i = i + 1
        return True


    def insert(self, table_name, args):
        with open('../data/' + table_name + '.csv', 'abr') as csvfile:
            user_writer = csv.writer(csvfile,
                                     delimiter=',',
                                     quotechar='|',
                                     quoting=csv.QUOTE_MINIMAL)
            table_data.table_data[table_name]['size'] = table_data.table_data[table_name]['size'] + 1
            row = [table_data.table_data[table_name]['size']]
            row.extend(args)
            user_writer.writerow(row)
            print str(row)


    def read(self, table_name, args):
        start = timer()
        results = []
        column_args = {}
        pos = 0
        for arg in args:
            if arg != '!' and arg != '*':
                column_args[pos] = arg
            pos += 1

        with open('../data/' + table_name + '.csv', 'rb') as csvfile:
            table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            print str(column_args)
            for row in table_reader:
                match = True
                for column_position in column_args.keys():
                    if row[column_position] != column_args[column_position]:
                        match = False
                        break

                if match:
                    results.append(row)

        end = timer()
        print(str(results))
        print(str(len(results)) + ' results returned in ' + str(end - start))


    def read_with_index(self, table_name, args):
        start = timer()
        indexed_columns = {}
        non_indexed_columns = {}
        results = []
        pos = 0
        for arg in args:
            if arg != '!' and arg != '*':
                column_name = table_data.table_data[table_name]["columns_pos"][pos]["name"]
                if column_name in table_data.indexes[table_name]:
                    indexed_columns[column_name] = arg
                else:
                    non_indexed_columns[pos] = arg
            pos += 1

        index_columns_len = len(indexed_columns.keys())
        indexed_column_results = []

        if index_columns_len > 0:
            for indexed_column in indexed_columns.keys():
                indexed_results = set()
                index = table_data.indexes[table_name][indexed_column]
                index_data_list = index[indexed_columns[indexed_column]]
                for index_result in index_data_list:
                    indexed_results.add(index_result)
                indexed_column_results.append(indexed_results)

        intersection_results = set()
        if len(indexed_column_results) > 1:
            i = 0
            while i < index_columns_len - 1:
                intersection_results = indexed_column_results[i].intersection(indexed_column_results[i + 1])
                i += 1
        else:
            intersection_results = indexed_column_results[0]

        indexed_results = []
        for intersection_result in intersection_results:
            index_data = intersection_result[0]
            page_index = intersection_result[1]
            with open(table_data.table_data[table_name]["data_store"] + index_data, 'rb') as csvfile:
                table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                indexed_results.append(list(table_reader)[page_index])

        if len(non_indexed_columns) > 0:
            for indexed_result in indexed_results:
                match = True
                for column_position in non_indexed_columns.keys():
                    if not indexed_result[column_position] == non_indexed_columns[column_position]:
                        match = False
                        break
                if match:
                    results.append(indexed_result)
        else:
            results = indexed_results

        end = timer()
        print(str(results))
        print(str(len(results)) + ' results returned in ' + str(end - start))
        return results



        end = timer()
        print(str(results))
        print(str(len(results)) + ' results returned in ' + str(end - start))


    def delete(self, table_name, delete_args):
        input_file = fileinput.input('../data/' + table_name + '.csv', inplace=True) # sys.stdout is redirected to the file
        print next(input_file), # write header as first line

        writer = csv.writer(sys.stdout, delimiter=',', quotechar='|')
        for row in csv.reader(input_file, delimiter=',', quotechar='|'):
            if row[0] != delete_args[0]:
                writer.writerow(row)


    def count(self, table_name, args):
        result = 0
        with open('../data/' + table_name + '.csv', 'rb') as csvfile:
            table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in table_reader:
                match = True
                i = 0
                for arg in args:
                    if row[i] is None or arg == '*':
                         i += 1
                    elif row[i] != arg:
                        match = False
                        break

                if match:
                    result = result + 1
        print(str(result))


    def sum(self, table_name, args):
        result = self.create_empty_int_result(args)
        with open('../data/' + table_name + '.csv', 'rb') as csvfile:
            table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in table_reader:
                row_result = self.create_empty_int_result(args)
                match = True
                i = 0
                for arg in args:
                    arg = arg.strip()
                    if arg == '!' or row[i] is None:
                        row_result[i] = '!'
                    elif arg == '*' and row[i].isdigit():
                        row_result[i] = row[i]
                    elif row[i] == arg:
                        row_result[i] = row[i]
                    elif row[i] != arg:
                        match = False
                        break
                    i += 1

                if match:
                    i = 0
                    for arg in args:
                        if arg == '*':
                            result[i] = result[i] + int(row_result[i])
                        else:
                            result[i] = row_result[i]
                        i += 1
        print(str(result))
