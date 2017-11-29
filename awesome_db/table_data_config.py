import json
import os
from os import listdir

import table_data


class TableConfig:

    def create_table_config(self):
        for table_config in listdir("../table_config/"):
            print "Found " + table_config
            if ".json" in table_config:
                with open("../table_config/" + table_config) as json_data:
                    data = json.loads(json_data.read())
                    if not os.path.isfile("../data/" + data["location"]):
                        print "Invalid data location - " + "../data/" + data["location"] + " - skipping"
                        continue

                    table_data.create_table_data(data)


