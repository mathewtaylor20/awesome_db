
# find the best filter query for that table column
# match on critera
# si & si + column_size

# si & mi + column_cardinality

# mi & mi + value_count

# si & lts + column_cardinality

# mi & lts + column_cardinality

# si & fts + loop size

# mi & fts + loop size

# lts & lts + column size

# lts & fts + loop size

# fts & fts + loop size
class FilterSelector:

    def eval_query(self, query_data):

       # print str(query_data)
        filter_results_dict = {}

        for table_name, column_values in query_data['tables'].iteritems():
            filter_results_dict[table_name] = {}
            for first_column_name, first_column_value in column_values.iteritems():
                filter_results_dict[table_name][first_column_name] = {}
                #print first_column_name + ' : ' + str(first_column_value)
                query_results = {}
                query_results[0] = []
                query_results[1] = []
                query_results[2] = []
                query_results[3] = []
                query_results[4] = []
                query_results[5] = []
                query_results[6] = []
                query_results[7] = []
                query_results[8] = []
                query_results[9] = []
                first_column_type = first_column_value['type']
                for second_column_name, second_column_value in column_values.iteritems():
                    if first_column_name == second_column_name:
                        continue

                    second_column_type = second_column_value['type']
                    if first_column_type == 'si' and second_column_type == 'si':
                        query_results[0].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'mi' and second_column_type == 'si') or
                          (first_column_type == 'si' and second_column_type == 'mi')):
                        query_results[1].append(query_data['tables'][table_name][second_column_name])
                    elif (first_column_type == 'mi' and second_column_type == 'mi'):
                        query_results[2].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'si' and second_column_type == 'lts') or
                          (first_column_type == 'lts' and second_column_type == 'si')):
                        query_results[3].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'mi' and second_column_type == 'lts') or
                              (first_column_type == 'lts' and second_column_type == 'mi')):
                        query_results[4].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'si' and second_column_type == 'fts') or
                         (first_column_type == 'fts' and second_column_type == 'si')):
                        query_results[5].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'mi' and second_column_type == 'fts') or
                              (first_column_type == 'fts' and second_column_type == 'mi')):
                        query_results[6].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'lts' and second_column_type == 'lts') or
                          (first_column_type == 'lts' and second_column_type == 'lts')):
                        query_results[7].append(query_data[table_name][second_column_name])
                    elif ((first_column_type == 'lts' and second_column_type == 'fts') or
                         (first_column_type == 'fts' and second_column_type == 'lts')):
                        query_results[8].append(query_data['tables'][table_name][second_column_name])
                    elif ((first_column_type == 'fts' and second_column_type == 'fts') or
                         (first_column_type == 'fts' and second_column_type == 'fts')):
                        query_results[9].append(query_data['tables'][table_name][second_column_name])
                    else:
                        print 'first_column_type ' + first_column_type + ' : ' + ' second_column_type ' + second_column_type

                query_score = 0
                for element in self.sub_score_query("result_factor",  query_results[0]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[1]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[2]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[3]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[4]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[5]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[6]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[7]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[8]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
                for element in self.sub_score_query("result_factor", query_results[9]):
                    self.set_results( query_data, filter_results_dict, table_name, first_column_name, element['column'], query_score)
                    query_score += 1
        return filter_results_dict


    def sub_score_query(self, sort_key, sub_query_data):
        sub_query_score = [(dict_[sort_key], dict_) for dict_ in sub_query_data]
        sub_query_score.sort()
        result = [dict_ for (key, dict_) in sub_query_score]
        return result


    def set_results(self, query_data, filter_results_dict, table_name, first_column_name, second_column_name, query_score):
        filter_results_dict[table_name][first_column_name][second_column_name] = {}
        filter_results_dict[table_name][first_column_name][second_column_name]['name'] = query_data['tables'][table_name][second_column_name]['name']
        filter_results_dict[table_name][first_column_name][second_column_name]['table'] = query_data['tables'][table_name][second_column_name]['table']
        filter_results_dict[table_name][first_column_name][second_column_name]['column'] = query_data['tables'][table_name][second_column_name]['column']
        filter_results_dict[table_name][first_column_name][second_column_name]['type'] = query_data['tables'][table_name][second_column_name]['type']
        filter_results_dict[table_name][first_column_name][second_column_name]['score'] = query_score
