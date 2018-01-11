import boto3, sys, json, time, uuid, datetime, botocore, re
import threading
from multiprocessing.pool import ThreadPool
import traceback

'''
Created on Aug, 2017
@author: sbellad
'''


def my_print(msg):
    print("{} {}".format(datetime.datetime.utcnow(), msg))


# thread local data
thread_data = threading.local()


class AthenaUtil(object):
    region_name = 'us-west-2'
    client = boto3.session.Session(region_name=region_name).client('athena')

    s3_staging_folder = None

    def __init__(self, s3_staging_folder):
        """ constructor requires s3 staging folder for storing results
        Parameters:
        s3_staging_folder = s3 folder with write permissions for storing results
        """
        self.s3_staging_folder = s3_staging_folder

    def execute_save_s3(self, sql_query, s3_output_folder):
        QueryExecutionId = self.start_query_execution(sql_query=sql_query, s3_output_folder=s3_output_folder)
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        if result_status_result["SUCCESS"]:
            return True
        else:
            raise Exception(result_status_result)

    def execute_query(self, sql_query, use_cache=False):
        """ executes query and returns results
        Parameters:
        sql_query = SQL query to execute on Athena
        use_cache = to reuse previous results if found (might give back stale results)

        Returns:
        ResultSet see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        """
        QueryExecutionId = self.start_query_execution(sql_query=sql_query, use_cache=use_cache)
        return self.get_results(QueryExecutionId=QueryExecutionId)

    def start_query_execution(self, sql_query,  use_cache=False, s3_output_folder=None):
        """ starts  query execution
        Parameters:
        sql_query = SQL query to execute on Athena
        use_cache = to reuse previous results if found (might give back stale results)

        Returns:
        QueryExecutionId that  identifies the query, can be used to get_results or get_results_by_page
        """
        outputLocation = self.s3_staging_folder if s3_output_folder is None else s3_output_folder
        print(outputLocation)
        response = self.client.start_query_execution(
            QueryString=sql_query,
            ClientRequestToken=str(uuid.uuid4()) if not use_cache else sql_query[:64].ljust(32) + str(hash(sql_query)),
            ResultConfiguration={
                'OutputLocation': outputLocation,
            }
        )
        return response["QueryExecutionId"]
        # return self.get_results(QueryExecutionId=response["QueryExecutionId"])

    def get_results(self, QueryExecutionId):
        """ waits for query to complete and returns results
        Parameters:
        QueryExecutionId that  identifies the query


        Returns:
        ResultSet see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        or Exception in case of error
        """
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        # print(result_status_result)
        result = None
        if result_status_result["SUCCESS"]:
            paginator = self.client.get_paginator('get_query_results')
            page_response = paginator.paginate(QueryExecutionId=QueryExecutionId)
            # PageResponse Holds 1000 objects at a time and will continue to repeat in chunks of 1000.

            for page_object in page_response:

                if result is None:
                    result = page_object
                    if result_status_result["QUERY_TYPE"] == "SELECT":
                        if len(result["ResultSet"]["Rows"]) > 0:
                            # removes column header from 1st row (Athena returns 1st row as col header)
                            del result["ResultSet"]["Rows"][0]
                else:
                    result["ResultSet"]["Rows"].extend(page_object["ResultSet"]["Rows"])
            result["ResponseMetadata"]["HTTPHeaders"]["content-length"] = None
            return result
        else:
            raise Exception(result_status_result)

    def __wait_for_query_to_complete(self, QueryExecutionId):
        """ private do not user, waits for query to execute """
        status = "QUEUED"  # assumed
        error_count = 0
        response = None
        while (status in ("QUEUED','RUNNING")):  # can be QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            try:
                response = self.client.get_query_execution(QueryExecutionId=QueryExecutionId)
                status = response["QueryExecution"]["Status"]["State"]
                # my_print(status)
                time.sleep(0.5)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('athena')

        if (status == "FAILED" or status == "CANCELLED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}
        else:
            return {"SUCCESS": True if response["QueryExecution"]["Status"]["State"] == "SUCCEEDED" else False,
                    "STATUS": response["QueryExecution"]["Status"]["State"]
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}

    def get_results_by_page(self, QueryExecutionId, NextToken=None, MaxResults=1000):
        """ waits for query to complete and returns result based on NextToken and MaxResults per page.
        Parameters:
        QueryExecutionId that  identifies the query
        NextToken from previous page of result set
        MaxResults max records per page

        Returns:
        ResultSet for a Page see http://boto3.readthedocs.io/en/latest/reference/services/athena.html#Athena.Client.get_query_results
        or Exception in case of error
        """
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        # print(result_status_result)
        if result_status_result["SUCCESS"]:
            if NextToken is None:
                MaxResults = MaxResults + 1
                result = self.client.get_query_results(QueryExecutionId=QueryExecutionId, MaxResults=MaxResults)
                # removes column header from 1st row (Athena returns 1st row as col header)

                if result_status_result["QUERY_TYPE"] == "SELECT":
                    if len(result["ResultSet"]["Rows"]) > 0:
                        del result["ResultSet"]["Rows"][0]
            else:
                result = self.client.get_query_results(QueryExecutionId=QueryExecutionId, NextToken=NextToken,
                                                       MaxResults=MaxResults)
            return result

    def get_table_partitions(self, table_name):
        """ queries and returns partitions of a table in Athena
        Parameters:
        table_name name of table name with database prefix

        Returns:
        array of string partitions
        """
        sql_query = "SHOW PARTITIONS " + table_name
        my_print("executing  athena SQL {}".format(sql_query))
        QueryExecutionId = self.start_query_execution(sql_query=sql_query)
        result = self.get_results(QueryExecutionId=QueryExecutionId)
        output = []
        for row in result["ResultSet"]["Rows"]:
            for data in row["Data"]:
                output.append(data["VarCharValue"])
        athena_table_partitions = sorted(output)
        return athena_table_partitions



    def start_query_execution_and_wait_for_completion(self, sql):
        """ starts a query execution an waits for it to complete, use this for last queries where u need a query id and want to download results page by page.
        Parameters:
        sql  query to execute

        Returns:
        query_status_result dictionary with following structure:
                    {"SUCCESS" : False | True,
                    "STATUS" :  status
                    , "QUERY_TYPE" : "FIRST WORD OF QUERY e.g. SELECT or INSERT or ALTER"
                    , "QUERY" : "ACTUAL QUERY"
                    , "StateChangeReason" : None | "Error string if any"}
        """
        query_status_result = None
        for attempt in range(3):
            try:
                util = None
                if hasattr(thread_data, 'util') == False:
                    thread_data.util = AthenaUtil(self.s3_staging_folder)

                util = thread_data.util
                result = util.start_query_execution(sql_query=sql)
                query_status_result = util.__wait_for_query_to_complete(result)
                # print(query_status_result)
                if query_status_result["SUCCESS"] == True:
                    return query_status_result
                else:
                    pass
                    # print("attempt",attempt,query_status_result, sql)
            except Exception as e:
                print("attempt", attempt, str(e), sql)
                time.sleep((attempt + 1) ** 2)
        return None

    def execute_sqls_threaded(self, sql_queries, thread_pool_size=5):
        """ executes a array of SQLs using threads and returns results, useful for threaded batch operations
        Parameters:
        sql_queries array of SQL queries to execute
        thread_pool_size pool size to use, MAX a/c limit in PROD is 50 so its recommended to keep it around 2-5.

        Returns:
        True if all SQLs have been executed successfully, else False
        """
        if len(sql_queries) == 0:
            return True

        start_time = time.time()

        if (thread_pool_size < 1):
            thread_pool_size = 1
        POOL_SIZE = thread_pool_size

        if (len(sql_queries) < POOL_SIZE):
            POOL_SIZE = len(sql_queries)
        # Make the Pool of workers
        pool = ThreadPool(POOL_SIZE)

        print("Using pool size of {}".format(POOL_SIZE))

        count = 0
        failed_count = 0
        OPERATIONS = len(sql_queries)
        result = True
        while ((count + failed_count) < OPERATIONS):
            # print(count,failed_count,OPERATIONS)
            try:
                for i, r in enumerate(
                        pool.imap_unordered(self.start_query_execution_and_wait_for_completion, sql_queries), 1):
                    try:
                        # print(i,r)

                        if r is None:
                            failed_count = failed_count + 1
                            result = False
                        elif "SUCCESS" in r and r["SUCCESS"] == False:
                            failed_count = failed_count + 1
                            result = False
                            # break
                        else:
                            print(r["QUERY"])
                            count += 1
                            # your code
                        # elapsed_time = time.time() - start_time
                        # sys.stderr.write('\r{0:%} {} {}'.format((count*1.0/OPERATIONS),count,elapsed_time))
                        sys.stderr.write(
                            '\r{0:%} completed {1}, failed {2}, TOTAL: {3}'.format((count * 1.0 / OPERATIONS), count,
                                                                                   failed_count, OPERATIONS))
                    except Exception as e:
                        # print(traceback.format_exc())
                        print("#", str(e))
                        failed_count += 1
                        # print('#',sys.exc_info()[1])
                        # pass
            except Exception as e:
                # print(traceback.format_exc())
                print(str(e))
                failed_count += 1
                # print('$',sys.exc_info()[1])
                pass
        print("test_threaded_metric_log --- %s seconds ---for %s get ops using %s threads" % (
        (time.time() - start_time), OPERATIONS, POOL_SIZE))
        print("total: " + str(OPERATIONS) + ", failed: " + str(failed_count))

        # close the pool and wait for the work to finish
        pool.close()
        pool.join()

        if ((result == True and count == OPERATIONS)):
            print("Operation successful")
            return True
        else:
            print("Operation had errors")
            raise Exception("Operation had errors")

    # utilities to convert result object in pandas dataframe

    def get_header(self, result):
        col = result['ResultSet']['ResultSetMetadata']['ColumnInfo']
        header = list(map(lambda x: x['Name'], col))
        # print(header)
        return header


    def get_pandas_frame(self, result):
        '''
        utilities to convert result object in pandas dataframe
        :param result: object returned by boto athena client containing data & meta data
        :return: pandas dataframe
            - df with column header & data
            - empty dataframe with columns header only in case of resultset is empty
            - none if irregular shape result object
            - throws error in any other case
        '''
        import pandas
        try:
            header = self.get_header(result)
            data = result['ResultSet']['Rows']
            print(data)
            if len(data) > 0 :
                df = pandas.io.json.json_normalize(data)
                df[header] = df['Data'].apply(pandas.Series)
                df = df.drop(['Data'], axis=1)
                df = df.applymap(lambda x: x['VarCharValue'])
            else:
                df = pandas.DataFrame(columns=header)

            return df
        except KeyError:
            print('No Data!')
            print(traceback.format_exc())
            return None

