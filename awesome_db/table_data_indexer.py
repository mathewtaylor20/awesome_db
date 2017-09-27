import csv
from os import listdir

from BTrees.OOBTree import OOBTree

import table_data


class TableIndexer:

    def index(self):
        for table_def in table_data.table_data.values():
            print ('Looking for Indexes on ' + table_def['name']) + '...'
            table_data.indexes[table_def["name"]] = {}
            for column_def in table_def["columns"].values():
                if column_def["index"]:
                    print 'Indexing table ' + table_def["name"] + ' column ' + column_def["name"]
                    index_tree = OOBTree()
                    table_data.indexes[table_def["name"]][column_def["name"]] = index_tree
                    i = 0
                    for data_file in listdir(table_def["data_store"]):
                        with open(table_def["data_store"] + data_file) as indexing_data:
                            page_index = 0
                            table_reader = csv.reader(indexing_data, delimiter=',', quotechar='|')
                            for row in table_reader:
                                index_data = row[column_def["position"]]
                                if not index_data in index_tree:
                                    index_tree.update({index_data : [(data_file, page_index)]})
                                else:
                                    index_tree[index_data].append((data_file, page_index))
                                page_index += 1
                        i += 1
                    print 'Indexed ' + str(i) + ' files...'