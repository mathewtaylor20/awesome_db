
class TableSelector:

    def eval_query(self, query_data):

        table_results_dict = {}
        for key, value in query_data['tables'].iteritems():

            query_results = {}
            query_results[0] = []
            query_results[1] = []
            query_results[2] = []
            query_results[3] = []
            query_results[4] = []
            query_results[5] = []
            query_results[6] = []
            query_results[7] = []

            for column_key, column_value in value.iteritems():
                if value[column_key]["type"] == 'si':
                    query_results[0].append(value[column_key])
                elif value[column_key]["type"] == 'mi' and value[column_key]["sub_type"] == 1:
                    query_results[1].append(value[column_key])
                elif value[column_key]["type"] == 'mi' and value[column_key]["sub_type"] == 2:
                    query_results[2].append(value[column_key])
                elif value[column_key]["type"] == 'mi' and value[column_key]["sub_type"] == 3:
                    query_results[3].append(value[column_key])
                elif value[column_key]["type"] == 'lts' and value[column_key]["sub_type"] == 1:
                    query_results[4].append(value[column_key])
                elif value[column_key]["type"] == 'lts' and value[column_key]["sub_type"] == 2:
                    query_results[5].append(value[column_key])
                elif value[column_key]["type"] ==  'fts' and value[column_key]["sub_type"] == 1:
                    query_results[6].append(value[column_key])
                elif value[column_key]["type"] == 'fts' and value[column_key]["sub_type"] == 2:
                    query_results[7].append(value[column_key])

            query_score = 0
            for element in self.sub_score_query("result_factor",  query_results[0]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'si')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[1]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'mi')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[2]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'mi')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[3]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'mi')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[4]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'lts')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[5]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'lts')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[6]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'fts')
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[7]):
                self.set_results(table_results_dict, element['table'], element['column'], query_score, 'fts')
                query_score += 1

        return table_results_dict


    def sub_score_query(self, sort_key, sub_query_data):
        sub_query_score = [(dict_[sort_key], dict_) for dict_ in sub_query_data]
        sub_query_score.sort()
        result = [dict_ for (key, dict_) in sub_query_score]
        return result


    def set_results(self, table_results_dict, table_key, column_key, score, type):
        if not table_key in table_results_dict:
            table_results_dict[table_key] = {}
        if not column_key in table_results_dict[table_key]:
            table_results_dict[table_key][column_key] = {}
        table_results_dict[table_key][column_key]['score'] = score
        table_results_dict[table_key][column_key]['type'] = type
