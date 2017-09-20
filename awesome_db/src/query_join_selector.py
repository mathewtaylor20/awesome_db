import table_data

class JoinSelector:

    def eval_query(self, query_data):

        join_results_dict = {}
        for join_data in query_data['joins']:
            right_data = join_data['right']
            left_data = join_data['left']

            left_data_type = table_data.table_data[left_data['table']]['columns'][left_data['column']]["type"]
            right_data_type = table_data.table_data[left_data['table']]['columns'][left_data['column']]["type"]

            # Merge-sort join
            # Both inputs are indexed
            if (left_data['column_indexed'] and right_data['column_indexed']):
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'merge'
                join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']

            elif ((left_data['column_cardinality'] == 100 or right_data['column_cardinality'] == 100) and
                  (right_data_type == 'int' and left_data_type == 'int')):
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'merge'
                join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']

            # Hash Join
            # large data inputs
            elif ((left_data['column_size'] > 1000 and right_data['column_size'] > 1000) or
                  (left_data['column_indexed'] and right_data['column_size'] > 1000) or
                  (left_data['column_size'] > 1000 and right_data['column_indexed'])):
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'hash'

                if left_data['column_size'] > right_data['column_size']:
                    join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                    join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                    join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                    join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']
                else:
                    join_results_dict[join_data['name']]['outer_table'] = join_data['right']['table']
                    join_results_dict[join_data['name']]['outer_column'] = join_data['right']['column']
                    join_results_dict[join_data['name']]['inner_table'] = join_data['left']['table']
                    join_results_dict[join_data['name']]['inner_column'] = join_data['left']['column']

            # Hash Join
            # large fraction of a small table needs to be joined.
            elif ((right_data['column_size'] > 0 and right_data['column_size'] < 1000 and right_data['column_cardinality'] < 75) or
                  (left_data['column_size'] > 0 and left_data['column_size'] < 1000 and left_data['column_cardinality'] < 75)):
                if left_data['column_size'] > right_data['column_size']:
                    join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                    join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                    join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                    join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']
                else:
                    join_results_dict[join_data['name']]['outer_table'] = join_data['right']['table']
                    join_results_dict[join_data['name']]['outer_column'] = join_data['right']['column']
                    join_results_dict[join_data['name']]['inner_table'] = join_data['left']['table']
                    join_results_dict[join_data['name']]['inner_column'] = join_data['left']['column']


            # Optimized nested loop
            # Small sets
            elif (right_data['column_size'] > 0 and right_data['column_size'] < 1000 and
                  left_data['column_size'] > 0 and left_data['column_size'] < 1000):
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'loop'
                join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']

            # Optimized nested loop
            # One Small and other indexed
            elif ((left_data['column_indexed'] and right_data['column_size'] > 0 and right_data['column_size'] < 1000) or
                  (right_data['column_indexed'] and left_data['column_size'] > 0 and left_data['column_size'] < 1000)):
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'loop'
                join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']

            # Default nested loop
            else:
                join_results_dict[join_data['name']] = {}
                join_results_dict[join_data['name']]['type'] = 'loop'
                join_results_dict[join_data['name']]['outer_table'] = join_data['left']['table']
                join_results_dict[join_data['name']]['outer_column'] = join_data['left']['column']
                join_results_dict[join_data['name']]['inner_table'] = join_data['right']['table']
                join_results_dict[join_data['name']]['inner_column'] = join_data['right']['column']

        return join_results_dict