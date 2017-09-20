import table_data

def create_query_data(query, indexing, stats):
    query_data = {}
    query_data['tables'] = {}
    for query_table in query["tables"]:
        query_data['tables'][query_table["name"]] = {}
        for query_column in query_table["columns"]:
            if query_column["values"][0] != "*" and query_column["values"][0] != "!":
                table_def = table_data.table_data[query_table["name"]]
                column_def = table_def["columns"][query_column["name"]]

                query_data['tables'][query_table["name"]][query_column["name"]] = {}
                query_table_column = query_data['tables'][query_table["name"]][query_column["name"]]
                query_table_column["name"] = query_table["name"] + "_" + query_column["name"]
                query_table_column["table"] = query_table["name"]
                query_table_column["column"] = query_column["name"]
                query_table_column["column_indexed"] = column_def["index"] if indexing else False
                query_table_column["column_size"] = column_def["size"] if stats else 0
                query_table_column["column_cardinality"] = column_def["cardinality"] if stats else 0
                query_table_column["value_count"] = len(query_column["values"]) if stats else 0
                query_table_column["loop_count"] = len(query_column["values"]) * column_def["size"] if stats else 0
                query_table_column["values"] = query_column["values"]
                query_table_column["position"] = column_def["position"]
                if query_table_column["column_indexed"] and query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
                    query_table_column['type'] = 'si'
                elif query_table_column["column_indexed"] and query_table_column["value_count"] == 1:
                    query_table_column['type'] = 'mi'
                    query_table_column['sub_type'] = 1
                elif query_table_column["column_indexed"] and query_table_column["column_cardinality"] == 1:
                    query_table_column['type'] = 'mi'
                    query_table_column['sub_type'] = 2
                elif query_table_column["column_indexed"]:
                    query_table_column['type'] = 'mi'
                    query_table_column['sub_type'] = 3
                elif query_table_column["value_count"] == 1 and query_table_column["column_cardinality"] == 1:
                    print query_table_column["column"]
                    query_table_column['type'] = 'lts'
                    query_table_column['sub_type'] = 1
                elif query_table_column["column_cardinality"] == 1:
                    print query_table_column["column"]
                    query_table_column['type'] = 'lts'
                    query_table_column['sub_type'] = 2
                elif query_table_column["value_count"] == 1:
                    query_table_column['type'] = 'fts'
                    query_table_column['sub_type'] = 1
                else:
                    query_table_column['type'] = 'fts'
                    query_table_column['sub_type'] = 2
                query_table_column['result_factor'] = get_result_factor(query_table_column["column_indexed"],
                                                                        query_table_column["column_size"],
                                                                        query_table_column["column_cardinality"],
                                                                        query_table_column["value_count"])

    query_data['joins'] = []
    for query_join in  query["joins"]:
        left_table_def = table_data.table_data[query_join["left_table"]]
        left_column_def = left_table_def["columns"][query_join["left_column"]]
        right_table_def = table_data.table_data[query_join["right_table"]]
        right_column_def = right_table_def["columns"][query_join["right_column"]]

        join_def = {}
        join_def["name"] = query_table["name"] + "_" + query_column["name"]
        join_def['left'] = {}
        join_def['left']["table"] = left_table_def["name"]
        join_def['left']["column"] = left_column_def["name"]
        join_def['left']["column_indexed"] = left_column_def["index"] if indexing else False
        join_def['left']["column_size"] = left_column_def["size"] if stats else 0
        join_def['left']["column_cardinality"] = left_column_def["cardinality"] if stats else 0

        join_def['right'] = {}
        join_def['right']["table"] = right_table_def["name"]
        join_def['right']["column"] = right_column_def["name"]
        join_def['right']["column_indexed"] = right_column_def["index"] if indexing else False
        join_def['right']["column_size"] = right_column_def["size"] if stats else 0
        join_def['right']["column_cardinality"] = right_column_def["cardinality"] if stats else 0

        query_data['joins'].append(join_def)

    return query_data


def get_result_factor(indexed, size, cardinality, values):
    result_factor = 0
    if indexed:
        result_factor = float(cardinality * values)
    elif cardinality == 1 or values == 1:
        result_factor = float(cardinality * values) * (float(size) * 0.5)
    else:
        result_factor = float(cardinality * values) * (float(size))
    return result_factor


