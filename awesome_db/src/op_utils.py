def get_matched_rows(table_reader, table_args):
    results = []
    for row in table_reader:
        match = True
        i = 0
        for arg in table_args:
            if row[i] is None or arg == '*':
                i += 1
            elif row[i] != arg:
                match = False
                break

        if match:
            results.append(row)
    return results


def get_matched_rows_as_hash(table_reader, table_args, hash_column):
    results = {}
    for row in table_reader:
        match = True
        i = 0
        for arg in table_args:
            if row[i] is None or arg == '*':
                i += 1
            elif row[i] != arg:
                match = False
                break

        if match:
            results[row[hash_column]] = row
    return results


def get_join_column(join_columns):
    join_column = 0
    for column in join_columns:
        if column == '*':
            break
        join_column = join_column + 1
    return join_column


def create_result_row(left_row, left_args, right_row, right_args):
    result_args = create_empty_join_result(left_args, right_args)
    result_row = create_empty_int_result(result_args)
    i = 0
    lj = 0
    for left_arg in left_args:
        if left_arg == '!':
            result_row[i] = '!'
        else:
            result_row[i] = left_row[lj]
        i = i + 1
        lj = lj + 1
    rj = 0
    for right_arg in right_args:
        if right_arg == '!':
            result_row[i] = '!'
        else:
            result_row[i] = right_row[rj]
        i = i + 1
        rj = rj + 1
    return result_row


def create_empty_join_result(left_args, right_args):
    result_args = []
    result_args.extend(left_args)
    result_args.extend(right_args)
    return result_args


def create_empty_int_result(params):
    result = []
    for param in params:
        result.append(0)
    return result