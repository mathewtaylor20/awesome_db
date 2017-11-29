from table_data_config import TableConfig
from table_data_file_splitter import TableFileSplitter
from table_data_indexer import TableIndexer
from table_data_stats import TableStats


class DBInitializer:

    def initalize_db(self, split_file, indexing, stats):
        print "Starting initialization..."

        print "Reading table config..."
        # Look for the file definitions in the table_config folder
        table_config = TableConfig()
        table_config.create_table_config()

        # Split the files and move into the data_store dir
        print "Splitting data files into pages..."
        table_file_splitter = TableFileSplitter()
        table_file_splitter.split_table_files(split_file)

        # Create indexes, calculate line count, cardinality and other stats
        if indexing:
            print "Creating indexes..."
            table_indexer = TableIndexer()
            table_indexer.index()

        if stats:
            print "Calculating cardinalities..."
            table_stats = TableStats()
            table_stats.create_stats()

        print "awesome_db initialized!"
        return True
