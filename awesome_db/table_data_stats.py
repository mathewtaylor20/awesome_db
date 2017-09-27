import csv
from os import listdir

import table_data


class TableStats:

    def create_stats(self):
        for table_def in table_data.table_data.values():
            print 'Reading data for ' + table_def['name'] + '...'
            column_cardinality = {}
            column_count = table_def["column_count"]
            j = 0
            while j < column_count:
                column_cardinality[j] = {}
                column_cardinality[j]["size"] = 0
                column_cardinality[j]["count"] = set()
                j += 1

            for data_file in listdir(table_def["data_store"]):
                with open(table_def["data_store"] + data_file) as carding_data:
                    table_reader = csv.reader(carding_data, delimiter=',', quotechar='|')
                    for row in table_reader:
                        i = 0
                        for value in row:
                            column_cardinality[i]["count"].add(value)
                            column_cardinality[i]["size"] += 1
                            i += 1

            for column_pos, value in column_cardinality.iteritems():
                print 'Table : ' + table_def["name"] + ' : ' + table_def["columns_pos"][column_pos]["name"] + ' : ' + \
                      ' : ' + str(len(value["count"])) + ' : ' + str(value["size"]) + \
                      ' : cardinality = ' + str(float(value["size"])/float(len(value["count"])))
                table_def["columns_pos"][column_pos]["cardinality"] = float(value["size"]/len(value["count"]))
                table_def["columns"][table_def["columns_pos"][column_pos]["name"]]["cardinality"] = float(value["size"]/len(value["count"]))
                table_def["columns_pos"][column_pos]["size"] = value["size"]
                table_def["columns"][table_def["columns_pos"][column_pos]["name"]]["size"] =value["size"]