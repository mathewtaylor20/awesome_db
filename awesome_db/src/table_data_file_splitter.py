import csv_splitter
import os
import table_data


class TableFileSplitter:

    def split_table_files(self):
        for table_def in table_data.table_data.values():
            print ('Creating ' + table_def['name']) + ' data store files...'
            if not os.path.isdir('../data_store/' + table_def["name"]):
                os.mkdir('../data_store/' + table_def["name"])
            csv_splitter.split(open('../data/' + table_def["data_loc"], 'r'),
                               ',',
                               100,
                               table_def["name"] + '_%s.csv',
                               '../data_store/' + table_def["name"],
                               False)
            table_def["data_store"] = '../data_store/' + table_def["name"] + '/'