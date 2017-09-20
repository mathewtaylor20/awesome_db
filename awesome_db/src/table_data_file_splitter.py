import csv_splitter
import os
import shutil
import table_data


class TableFileSplitter:

    def split_table_files(self, split_file):
        if os.path.isdir('../data_store/'):
            shutil.rmtree('../data_store/')
        os.mkdir('../data_store/')
        for table_def in table_data.table_data.values():
            print ('Creating ' + table_def['name']) + ' data store files...'
            if not os.path.isdir('../data_store/' + table_def["name"]):
                os.mkdir('../data_store/' + table_def["name"])
            if split_file:
                csv_splitter.split(open('../data/' + table_def["data_loc"], 'r'),
                                   ',',
                                   100,
                                   table_def["name"] + '_%s.csv',
                                   '../data_store/' + table_def["name"],
                                   False)
            else:
                shutil.copy2('../data/' + table_def["data_loc"], '../data_store/' + table_def["name"])
            table_def["data_store"] = '../data_store/' + table_def["name"] + '/'
            table_def["data_files"] = os.listdir('../data_store/' + table_def["name"] + '/')