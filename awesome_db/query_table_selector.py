
class TableSelector:

    def eval_query(self, query_data):

        table_results_dict = {}
        self.score_query(table_results_dict, query_data["tables"])
        return table_results_dict

    def score_query(self, table_results_dict, query_data_obj):
        for key, value in query_data_obj.iteritems():

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
                if value[column_key]["type"] == "si":
                    query_results[0].append(value[column_key])
                elif value[column_key]["type"] == "mi" and value[column_key]["sub_type"] == 1:
                    query_results[1].append(value[column_key])
                elif value[column_key]["type"] == "mi" and value[column_key]["sub_type"] == 2:
                    query_results[2].append(value[column_key])
                elif value[column_key]["type"] == "mi" and value[column_key]["sub_type"] == 3:
                    query_results[3].append(value[column_key])
                elif value[column_key]["type"] == "lts" and value[column_key]["sub_type"] == 1:
                    query_results[4].append(value[column_key])
                elif value[column_key]["type"] == "lts" and value[column_key]["sub_type"] == 2:
                    query_results[5].append(value[column_key])
                elif value[column_key]["type"] ==  "fts" and value[column_key]["sub_type"] == 1:
                    query_results[6].append(value[column_key])
                elif value[column_key]["type"] == "fts" and value[column_key]["sub_type"] == 2:
                    query_results[7].append(value[column_key])

            query_score = 0
            for element in self.sub_score_query("result_factor",  query_results[0]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "si", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[1]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "mi", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[2]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "mi", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[3]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "mi", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[4]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "lts", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[5]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "lts", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[6]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "fts", element["is_join"], element["column_id"])
                query_score += 1
            for element in self.sub_score_query("result_factor", query_results[7]):
                self.set_results(table_results_dict, element["table"], element["column"], query_score, "fts", element["is_join"], element["column_id"])
                query_score += 1



    def sub_score_query(self, sort_key, sub_query_data):
        sub_query_score = [(dict_[sort_key], dict_) for dict_ in sub_query_data]
        sub_query_score.sort()
        result = [dict_ for (key, dict_) in sub_query_score]
        return result


    def set_results(self, table_results_dict, table_key, column_key, score, type, is_join, select_id):
        if not table_key in table_results_dict:
            table_results_dict[table_key] = {}
        if not column_key in table_results_dict[table_key]:
            table_results_dict[table_key][column_key] = {}
        table_results_dict[table_key][column_key]["score"] = score
        table_results_dict[table_key][column_key]["type"] = type
        table_results_dict[table_key][column_key]["is_join"] = is_join
        table_results_dict[table_key][column_key]["select_id"] = select_id
