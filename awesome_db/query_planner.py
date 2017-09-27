from timeit import default_timer as timer

from query_filter_selector import FilterSelector
from query_table_selector import TableSelector
from query_join_selector import JoinSelector

class QueryPlanner:

    def eval_query(self, query_data):

        start = timer()
        query_results = {}
        #print '\n\nQuery data : ' + str(query_data)

        table_selector = TableSelector()
        table_results = table_selector.eval_query(query_data)
        query_results['selects'] = table_results
        #print '\n\nTabled query results : ' + str(table_results)

        filter_selector = FilterSelector()
        filter_results = filter_selector.eval_query(query_data)
        query_results['filters'] = filter_results
        #print '\n\nFiltered query results : ' + str(filter_results)

        join_selector = JoinSelector()
        join_results = join_selector.eval_query(query_data)
        query_results['joins'] = join_results
        #print '\n\nFiltered join results : ' + str(join_results)

        end = timer()
        # print '\n\n' + str(results)
        print('\n\nQuery planned in ' + str(end - start))
        return query_results

