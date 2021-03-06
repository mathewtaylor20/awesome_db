import table_data

class QueryData:

    def create_query_data(self, query, indexing, stats, optimzied):
        query_data = {}
        query_data["tables"] = {}
        for query_table in query["tables"]:
            query_data["tables"][query_table["name"]] = {}
            for query_column in query_table["columns"]:
                if query_column["values"][0] != "*" and query_column["values"][0] != "!":
                    table_def = table_data.table_data[query_table["name"]]
                    column_def = table_def["columns"][query_column["name"]]
                    self.get_query_data(query_data, table_def, column_def, query_column["values"], indexing, stats)

        for query_join in query["joins"]:
            self.get_join_data(query_data, query_join, indexing, stats)

        if optimzied:
            for query_join in query["joins"]:
                self.get_optimised_fetch(query_data, query_join, indexing, stats)
        return query_data


    def get_query_data(self, query_data, table_def, column_def, values, indexing, stats):
        query_data["tables"][table_def["name"]][column_def["name"]] = {}
        query_table_column = query_data["tables"][table_def["name"]][column_def["name"]]
        query_table_column["name"] = table_def["name"] + "_" + column_def["name"]
        query_table_column["table"] = table_def["name"]
        query_table_column["column"] = column_def["name"]
        query_table_column["column_id"] = column_def["name"]
        query_table_column["is_join"] = False
        query_table_column["column_indexed"] = column_def["index"] if indexing else False
        query_table_column["column_size"] = column_def["size"] if stats else 0
        query_table_column["column_cardinality"] = column_def["cardinality"] if stats else 0
        query_table_column["value_count"] = len(values) if stats else 0
        query_table_column["loop_count"] = len(values) * column_def["size"] if stats else 0
        query_table_column["values"] = values
        query_table_column["position"] = column_def["position"]
        if query_table_column["column_indexed"] and query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
            query_table_column["type"] = "si"
        elif query_table_column["column_indexed"] and query_table_column["value_count"] == 1:
            query_table_column["type"] = "mi"
            query_table_column["sub_type"] = 1
        elif query_table_column["column_indexed"] and query_table_column["column_cardinality"] == 1:
            query_table_column["type"] = "mi"
            query_table_column["sub_type"] = 2
        elif query_table_column["column_indexed"]:
            query_table_column["type"] = "mi"
            query_table_column["sub_type"] = 3
        elif query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
            query_table_column["type"] = "lts"
            query_table_column["sub_type"] = 1
        elif query_table_column["column_cardinality"] == 1:
            query_table_column["type"] = "lts"
            query_table_column["sub_type"] = 2
        elif query_table_column["value_count"] == 1:
            query_table_column["type"] = "fts"
            query_table_column["sub_type"] = 1
        else:
            query_table_column["type"] = "fts"
            query_table_column["sub_type"] = 2
        query_table_column["result_factor"] = self.get_result_factor(query_table_column["column_indexed"],
                                                                query_table_column["column_size"],
                                                                query_table_column["column_cardinality"],
                                                                query_table_column["value_count"])


    def get_join_data(self, query_data, query_join, indexing, stats):

        query_data["joins"] = []
        left_table_def = table_data.table_data[query_join["left_table"]]
        left_column_def = left_table_def["columns"][query_join["left_column"]]
        right_table_def = table_data.table_data[query_join["right_table"]]
        right_column_def = right_table_def["columns"][query_join["right_column"]]

        join_def = {}
        join_def["name"] = query_join["left_table"] + "_" + query_join["right_table"]
        join_def["left"] = {}
        join_def["left"]["table"] = left_table_def["name"]
        join_def["left"]["column"] = left_column_def["name"]
        join_def["left"]["column_indexed"] = left_column_def["index"] if indexing else False
        join_def["left"]["column_size"] = left_column_def["size"] if stats else 0
        join_def["left"]["column_cardinality"] = left_column_def["cardinality"] if stats else 0

        join_def["right"] = {}
        join_def["right"]["table"] = right_table_def["name"]
        join_def["right"]["column"] = right_column_def["name"]
        join_def["right"]["column_indexed"] = right_column_def["index"] if indexing else False
        join_def["right"]["column_size"] = right_column_def["size"] if stats else 0
        join_def["right"]["column_cardinality"] = right_column_def["cardinality"] if stats else 0

        query_data["joins"].append(join_def)


    def get_optimised_fetch(self, query_data, query_join, indexing, stats):

        left_table_def = table_data.table_data[query_join["left_table"]]
        left_column_def = left_table_def["columns"][query_join["left_column"]]
        right_table_def = table_data.table_data[query_join["right_table"]]
        right_column_def = right_table_def["columns"][query_join["right_column"]]

        self.get_optimised_fetch_data(query_data, left_table_def, left_column_def, query_join["right_table"], query_join["right_column"], indexing, stats)
        self.get_optimised_fetch_data(query_data, right_table_def, right_column_def, query_join["left_table"], query_join["left_column"], indexing, stats)


    def get_optimised_fetch_data(self, query_data, table_def, column_def, join_table_name, join_column_name, indexing, stats):

        join_table_def = query_data["tables"][join_table_name]
        for join_column, join_column_def in join_table_def.iteritems():
            query_table_column = {}

            if "value_count" not in query_table_column or query_table_column["value_count"] > int(join_column_def["value_count"] * join_column_def["column_cardinality"]):
                query_table_column["name"] = table_def["name"] + "_" + column_def["name"]
                query_table_column["table"] = table_def["name"]
                query_table_column["column"] = column_def["name"]
                query_table_column["column_id"] = column_def["name"] + "_" + join_column_name
                query_table_column["join_column"] = join_column_name
                query_table_column["join_table"] = join_table_name
                query_table_column["is_join"] = True
                query_table_column["column_indexed"] = column_def["index"] if indexing else False
                query_table_column["column_size"] = column_def["size"] if stats else 0
                query_table_column["column_cardinality"] = column_def["cardinality"] if stats else 0
                query_table_column["value_count"] = int(join_column_def["value_count"])

                query_table_column["values"] = ""
                query_table_column["position"] = column_def["position"]
                if query_table_column["column_indexed"] and query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
                    query_table_column["type"] = "si"
                elif query_table_column["column_indexed"] and query_table_column["value_count"] == 1:
                    query_table_column["type"] = "mi"
                    query_table_column["sub_type"] = 1
                elif query_table_column["column_indexed"] and query_table_column["column_cardinality"] == 1:
                    query_table_column["type"] = "mi"
                    query_table_column["sub_type"] = 1
                elif query_table_column["column_indexed"]:
                    query_table_column["type"] = "mi"
                    query_table_column["sub_type"] = 3
                elif query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
                    query_table_column["type"] = "lts"
                    query_table_column["sub_type"] = 1
                elif query_table_column["column_cardinality"] == 1:
                    query_table_column["type"] = "lts"
                    query_table_column["sub_type"] = 2
                elif query_table_column["value_count"] == 1:
                    query_table_column["type"] = "fts"
                    query_table_column["sub_type"] = 1
                else:
                    query_table_column["type"] = "fts"
                    query_table_column["sub_type"] = 2
                query_table_column["result_factor"] = self.get_result_factor(query_table_column["column_indexed"],
                                                                        query_table_column["column_size"],
                                                                        query_table_column["column_cardinality"],
                                                                        query_table_column["value_count"])

        query_data["tables"][table_def["name"]][column_def["name"]] = {}
        query_data["tables"][table_def["name"]][column_def["name"]] = query_table_column


    def get_result_factor(self, indexed, size, cardinality, value_count):
        if indexed:
            result_factor = cardinality * value_count
        elif cardinality == 1:
            result_factor = int(float(value_count) * (float(size) * 0.5))
        elif value_count == 1:
            result_factor = size
        else:
            result_factor = value_count * size
        return int(result_factor)


