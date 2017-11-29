import ast

table_data = {}
indexes = {}


def create_table_data(data):
    table_name = data["table"]
    print table_name + " table file present"
    table_data[table_name] = {}
    table_data[table_name]["name"] = table_name
    table_data[table_name]["data_loc"] = data["location"]
    table_data[table_name]["column_count"] = 0
    table_data[table_name]["size"] = 0
    position = 0
    table_data[table_name]["columns"] = {}
    table_data[table_name]["columns_pos"] = {}
    for column in data["columns"]:
        table_data[table_name]["columns"][column["name"]] = {}
        table_data[table_name]["columns"][column["name"]]["name"] = column["name"]
        table_data[table_name]["columns"][column["name"]]["type"] = column["type"]
        table_data[table_name]["columns"][column["name"]]["index"] = ast.literal_eval(column["index"])
        table_data[table_name]["columns"][column["name"]]["position"] = position
        table_data[table_name]["columns"][column["name"]]["cardinality"] = 0
        table_data[table_name]["columns_pos"][position] = {}
        table_data[table_name]["columns_pos"][position]["name"] = column["name"]
        table_data[table_name]["columns_pos"][position]["type"] = column["type"]
        table_data[table_name]["columns_pos"][position]["index"] = ast.literal_eval(column["index"])
        table_data[table_name]["columns_pos"][position]["cardinality"] = 0
        table_data[table_name]["column_count"] += 1
        position = position + 1