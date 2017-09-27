import csv

from op_utils import get_join_column
from op_utils import get_matched_rows_as_hash
from op_utils import get_matched_rows
from op_utils import create_result_row
from timeit import default_timer as timer


def hash_join(left_table_name, left_args, left_join_columns, right_table_name, right_args, right_join_columns):
    start = timer()

    left_join_column = get_join_column(left_join_columns)
    right_join_column = get_join_column(right_join_columns)

    with open('../data/' + right_table_name + '.csv', 'rb') as csvfile:
        right_table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        right_results = get_matched_rows_as_hash(right_table_reader, right_args, right_join_column)

    with open('../data/' + left_table_name + '.csv', 'rb') as csvfile:
        left_table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        left_results = get_matched_rows(left_table_reader, left_args)

    results = []
    for left_row in left_results:
        if left_row[left_join_column] in right_results.keys():
            right_row = right_results[left_row[left_join_column]]
            result_row = create_result_row(left_row, left_args, right_row, right_args)
            results.append(result_row)

    end = timer()
    #  print(results)
    print(str(len(results)) + ' results returned in ' + str(end - start))