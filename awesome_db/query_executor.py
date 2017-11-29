import csv
from timeit import default_timer as timer

import table_data


class QueryExecutor:

    def execute_query(self, query_data, query_plan, indexing):

        start = timer()
        results = []

        # Iterate over selects
        table_select_results = {}
        start_selects = timer()
        for table_name, table_select in query_plan["selects"].iteritems():
            start_sub_select = timer()
            column_name, column_plan = self.get_column_plan(table_select, 0)
            print "\n\ntable: " + table_name + ", column: " + column_name + ", type: " + str(column_plan["type"]) + \
                  ", score: " + str(column_plan["score"]) + ", is join: " + str(column_plan["is_join"])
            table_select_plan = {}
            table_select_plan["table"] = table_name
            table_select_plan["column"] = column_name
            table_select_plan["position"] = query_data["tables"][table_name][column_name]["position"]
            table_select_plan["type"] = column_plan["type"]
            if column_plan["is_join"]:
                join_table = query_data["tables"][table_name][column_name]['join_table']
                join_column = query_data["tables"][table_name][column_name]['join_column']
                join_result_list = []
                join_column_position = query_data["tables"][join_table][join_column]["position"]
                for result_entry in table_select_results[join_table]:
                    join_result_list.append(result_entry[join_column_position])
                table_select_plan["values"] = join_result_list
            else:
                table_select_plan["values"] = query_data["tables"][table_name][column_name]["values"]
            print "Select : " + str(table_select_plan)
            table_file_set = self.get_data_files_list(query_data, table_select_plan)

            # Iterate over filters
            filter_score = 0
            while filter_score < len(query_plan["filters"][table_name][column_name]):
                filter_name, filter_plan = self.get_column_plan(query_plan["filters"][table_name][column_name], filter_score)
                if filter_plan["type"] == "si" or filter_plan["type"] == "mi":
                    print "Applying indexed filters"
                    filter_select_plan = {}
                    filter_select_plan["table"] = table_name
                    filter_select_plan["column"] = filter_name
                    filter_select_plan["type"] = filter_plan["type"]
                    filter_select_plan["values"] = query_data["tables"][table_name][filter_name]["values"]
                    filter_select_plan["value_count"] = len(filter_select_plan["values"])
                    filter_score = filter_score + 1
                    filter_file_set = self.get_data_files_list(query_data, filter_select_plan)
                    table_file_set = table_file_set.intersection(filter_file_set)
                else:
                    break

            filter_select_plan = []
            while filter_score < len(query_plan["filters"][table_name][column_name]):
                filter_name, filter_plan = self.get_column_plan(query_plan["filters"][table_name][column_name], filter_score)
                if filter_plan["type"] == "lts" or filter_plan["type"] == "fts":
                    print "Applying table scan filters"
                    filter = {}
                    filter["column"] = filter_name
                    filter["position"] = query_data["tables"][table_name][filter_name]["position"]
                    filter["type"] = filter_plan["type"]
                    filter["values"] = query_data["tables"][table_name][filter_name]["values"]
                    filter["value_count"] = len(filter["values"])
                    filter_select_plan.append(filter)
                    filter_score = filter_score + 1
                else:
                    break
            print "Pre Selection file_set size : " + str(len(table_file_set))
            print "Filter : " + str(filter_select_plan)
            reduced_file_set = self.reduce_file_set(table_file_set, ((table_select_plan["type"] == "si" or
                                                                      table_select_plan["type"] == "mi") and
                                                                      indexing))
            print "Selection file set size : " + str(len(reduced_file_set))
            filtered_select_results = []
            table_select_results[table_name] = filtered_select_results
            end_sub_select = timer()
            print "Sub select run time : " + str(end_sub_select - start_sub_select)
            self.multi_file_read(reduced_file_set, table_select_plan, filter_select_plan, table_name, filtered_select_results)
            print "Filtered results set size : " + str(len(filtered_select_results))
        end_selects = timer()
        print "\nSelect run time : " + str(end_selects - start_selects)

        # Iterate over Joins
        start_joins = timer()
        for join_name, join_plan in query_plan["joins"].iteritems():
            start_sub_join = timer()
            if join_plan["type"] == "hash":
                print "\n\nUsing Hash join - inner [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + \
                        "] to outer [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + "]"
                results = self.create_hash_join(join_plan,
                                                table_select_results[join_plan["outer_table"]],
                                                table_select_results[join_plan["inner_table"]])

            elif join_plan["type"] == "merge":
                print "\n\nUsing Merge join - inner [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + \
                      "] to outer [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + "]"
                results = self.create_merge_join(join_plan,
                                                 table_select_results[join_plan["outer_table"]],
                                                 table_select_results[join_plan["inner_table"]])

            elif join_plan["type"] == "loop":
                print "\n\nUsing Loop join - inner [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + \
                      "] to outer [" + table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["name"] + "]"
                results = self.create_loop_join(join_plan,
                                                 table_select_results[join_plan["outer_table"]],
                                                 table_select_results[join_plan["inner_table"]])
            end_sub_join = timer()
            print "Sub join run time : " + str(end_sub_join - start_sub_join)
        end_joins = timer()
        print "Joins run time : " + str(end_joins - start_joins)


        end = timer()
        print(str(len(results)) + " results found in " + str(end - start))
        return results


    def create_hash_join(self, join_plan, outer_data, inner_data):
        hashed_data = {}
        inner_column_pos = table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["position"]
        outer_column_pos = table_data.table_data[join_plan["outer_table"]]["columns"][join_plan["outer_column"]]["position"]
        for inner_row in inner_data:
            hashed_data[inner_row[inner_column_pos]] = inner_row

        results = []
        for outer_row in outer_data:
            if outer_row[outer_column_pos] in hashed_data.keys():
                combined_results = outer_row
                combined_results.extend(hashed_data[outer_row[outer_column_pos]])
                results.append(combined_results)
        return results


    def create_loop_join(self, join_plan, outer_data, inner_data):
        inner_column_pos = table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["position"]
        outer_column_pos = table_data.table_data[join_plan["outer_table"]]["columns"][join_plan["outer_column"]]["position"]
        results = []
        for outer_row in outer_data:
            for inner_row in inner_data:
                if outer_row[outer_column_pos] == inner_row[inner_column_pos]:
                    combined_results = outer_row
                    combined_results.extend(inner_row)
                    results.append(combined_results)
        return results


    def create_merge_join(self, join_plan, outer_data, inner_data):
        inner_column_pos = table_data.table_data[join_plan["inner_table"]]["columns"][join_plan["inner_column"]]["position"]
        outer_column_pos = table_data.table_data[join_plan["outer_table"]]["columns"][join_plan["outer_column"]]["position"]
        results = []

        outer_data.sort(key=lambda x: x[outer_column_pos])
        inner_data.sort(key=lambda x: x[inner_column_pos])
        inner_index = 0
        for outer_row in outer_data:
            if inner_index >= len(inner_data):
                break
            while True:
                if inner_index >= len(inner_data):
                    break
                inner_row = inner_data[inner_index]
                if inner_row[inner_column_pos] > outer_row[outer_column_pos]:
                    break
                elif outer_row[outer_column_pos] == inner_row[inner_column_pos]:
                    combined_results = outer_row
                    combined_results.extend(inner_row)
                    results.append(combined_results)
                    break
                else:
                    inner_index = inner_index + 1
        return results


    def get_data_files_list(self, query_data, select_plan):
        if select_plan["type"] == "si":
            return self.get_indexed_file_list(query_data, select_plan)
        elif select_plan["type"] == "mi":
            return self.get_indexed_file_list(query_data, select_plan)
        elif select_plan["type"] == "lts":
            return self.get_file_list(query_data, select_plan)
        elif select_plan["type"] == "fts":
            return self.get_file_list(query_data, select_plan)


    def get_indexed_file_list(self, query_data, table_select_plan):
        print "Creating indexed file list"
        files_list = []
        index = table_data.indexes[table_select_plan["table"]][table_select_plan["column"]]
        for value in table_select_plan["values"]:
            files_list.extend(index[str(value)])
        files_set = set()
        files_set.update(files_list)
        return files_set


    def get_file_list(self, query_data, table_select_plan):
        print "Creating non-indexed file list"
        files_set = set()
        files_set.update(table_data.table_data[table_select_plan["table"]]["data_files"])
        return files_set


    def get_column_plan(self, table_select, score):
        for column_name, column_plan in table_select.iteritems():
            if column_plan["score"] == score:
                return column_name, column_plan


    def multi_file_read(self, table_file_set, table_select_plan, filter_select_plan, table_name, filtered_select_results):
        start = timer()
        for file_name, line_values in table_file_set.iteritems():
            if "*" not in line_values :
                table_data.table_data[table_name]
                with open(table_data.table_data[table_name]["data_store"] + file_name, "rb") as csvfile:
                    table_reader = csv.reader(csvfile, delimiter=",", quotechar="|")
                    for line_number, result_line in enumerate(table_reader):
                        if line_number in line_values:
                            line_values.remove(line_number)
                            matched = True
                            for filter_plan in filter_select_plan:
                                if filter_plan["value_count"] == 0:
                                    return
                                data_type = self.get_data_type(table_select_plan["table"], filter_plan["column"])
                                read_val = result_line[filter_plan["position"]]
                                if data_type == "int":
                                    read_val = int(read_val)
                                if read_val in filter_plan["values"]:
                                    if filter_plan["type"] == "lts":
                                        filter_plan["value_count"] = filter_plan["value_count"] -1
                                    continue
                                else:
                                    matched = False
                            if matched:
                                filtered_select_results.append(result_line)
                        if not line_values:
                            continue

            else:
                with open(table_data.table_data[table_name]["data_store"] + file_name, "rb") as csvfile:
                    table_reader = csv.reader(csvfile, delimiter=",", quotechar="|")
                    for result_line in table_reader:
                        data_type = self.get_data_type(table_select_plan["table"], table_select_plan["column"])
                        read_val = result_line[table_select_plan["position"]]
                        if data_type == "int":
                            read_val = int(read_val)
                        if read_val in table_select_plan["values"]:
                            matched = True
                            for filter_plan in filter_select_plan:
                                if filter_plan["value_count"] == 0:
                                    return
                                data_type = self.get_data_type(table_select_plan["table"], filter_plan["column"])
                                read_val = result_line[filter_plan["position"]]
                                if data_type == "int":
                                    read_val = int(read_val)
                                if read_val in filter_plan["values"]:
                                    if filter_plan["type"] == "lts":
                                        filter_plan["value_count"] = filter_plan["value_count"] - 1
                                    continue
                                else:
                                    matched = False
                            if matched:
                                filtered_select_results.append(result_line)
                        else:
                            continue
        end = timer()
        print("File read run time : " + str(end - start))
        return


    def reduce_file_set(self, table_file_set, is_indexed):
        reduced_file_list = {}
        if is_indexed:
            for file_name, line_num in table_file_set:
                if file_name not in reduced_file_list:
                    reduced_file_list[file_name] = set()
                reduced_file_list[file_name].add(line_num)
        else:
            for file_name in table_file_set:
                if file_name not in reduced_file_list:
                    reduced_file_list[file_name] = set()
                reduced_file_list[file_name].add("*")
        return reduced_file_list



    def get_data_type(self, table_name, column_name):
        return table_data.table_data[table_name]["columns"][column_name]["type"]