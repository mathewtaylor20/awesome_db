import csv

from timeit import default_timer as timer


def merge_join(self, left_table_name, left_args, left_join_columns, right_table_name, right_args, right_join_columns):
    start = timer()

    left_join_column = self.get_join_column(left_join_columns)
    right_join_column = self.get_join_column(right_join_columns)

    with open('../data/' + left_table_name + '.csv', 'rb') as csvfile:
        left_table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        left_results = self.get_matched_rows(left_table_reader, left_args)
        left_results.sort(key=lambda x: x[left_join_column])

    print str(len(left_results))
    with open('../data/' + right_table_name + '.csv', 'rb') as csvfile:
        right_table_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        right_results = self.get_matched_rows(right_table_reader, right_args)
        right_results.sort(key=lambda x: x[right_join_column])

    results = []
    right_index = 0
    for left_row in left_results:
        if right_index > len(right_results):
            print('1')
            break
        while True:
            if right_index > len(right_results):
                print('2')
                break
            right_row = right_results[right_index]
            print('3 ' + str(right_row))
            if right_row[right_join_column] > left_row[left_join_column]:
                print '4'
                break
            elif left_row[left_join_column] == right_row[right_join_column]:
                result_row = self.create_result_row(left_row, left_args, right_row, right_args)
                results.append(result_row)
                print '5'
                break
            else:
                right_index = right_index + 1
                print '6 :' + str(right_index)

    end = timer()
    #   print(results)
    print(str(len(results)) + ' results returned in ' + str(end - start))