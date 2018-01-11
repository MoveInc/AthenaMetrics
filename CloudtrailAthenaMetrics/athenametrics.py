from athena_util import AthenaUtil
import argparse
import boto3
import datetime
import sys

'''
Collect Athena query IDs from cloudtrail and
query the Athena API for those query IDs
'''

def parse_arguments():
  parser = argparse.ArgumentParser(
                  description='Collect Athena data usage metrics')
  parser.add_argument('-s', '--staging-folder',
                        help='staging folder for Athena',
                        required=True)
  parser.add_argument('-b', '--destination-bucket',
                        help='an existing bucket where the metrics are saved.',
                        required=True)
  return parser



def splitarr(array, chunksize):
  ''' Split the array in sub arrays of size chunksize.
  return the array of these sub-arrays.
  '''
  ret = []
  lower = 0
  for j in range(lower, len(array)):
    tmp = []
    upper = lower + chunksize
    if( upper >= len(array) ):
      upper = len(array)
    for k in range(lower,upper):
      tmp.append(array[k])
    lower = upper
    ret.append(tmp)
    if( upper >= len(array) ) :
      return ret


def main():
  parser = parse_arguments()
  if __name__ == '__main__':
    args = parser.parse_args()
    collect_metrics( args.staging_folder, args.destination_bucket )


def collect_metrics(staging_folder, destination_bucket):
  ''' Query Cloudtrail to extract queryIDs and iterate the queryIDs
      in batches of 50. For each queryID, invoke the Athena API to
      get athena usage information.
  '''
  client = boto3.client('athena', region_name='us-west-2')

  ## The Athena query on cloudtrails table to get last days' query IDs
  query_str = """
  with data as
  (SELECT json_extract(responseelements, '$.queryExecutionId') as query_id,
          (useridentity.arn) as uid,
          (useridentity.sessioncontext.sessionIssuer.userName) as role,
          from_iso8601_timestamp(eventtime) as dt
  FROM default."cloudtrail_logs"
  WHERE eventsource='athena.amazonaws.com'
  AND eventname='StartQueryExecution' 
  AND json_extract(responseelements, '$.queryExecutionId') is not null)
  SELECT * FROM data WHERE dt >  date_add('day',-1,now()  )
  """

  result = AthenaUtil(s3_staging_folder = staging_folder) \
              .execute_query(query_str)

  query_ids =[]
  for row in result["ResultSet"]["Rows"]:
      data = row['Data'][0]
      print(row)
      item = data['VarCharValue'].strip('"')
      query_ids.append(item)

  allqueryids = (query_ids[1:])

  i = 0
  ## Iterate in batches of 50. That is the default Athena limit per account.
  for batchqueryids in splitarr(allqueryids,50):
      try:
          response = client.batch_get_query_execution(
          QueryExecutionIds=batchqueryids
          )
          athena_metrics = ""
          for row in response["QueryExecutions"]:
              queryid = row['QueryExecutionId']
              querydatabase = "null"
              if 'QueryExecutionContext' in row and 'Database' in row['QueryExecutionContext']:
                  querydatabase = row['QueryExecutionContext']['Database']
              executiontime = "null"
              if 'EngineExecutionTimeInMillis' in row['Statistics']:
                  executiontime = str(row['Statistics']['EngineExecutionTimeInMillis'])
              datascanned = "null"
              if 'DataScannedInBytes' in row['Statistics']:
                  datascanned = str(row ['Statistics']['DataScannedInBytes'])
              status = row ['Status']['State']
              submissiondatetime="null"
              if 'SubmissionDateTime' in row['Status']:
                  submissiondatetime = str(row['Status']['SubmissionDateTime'])
              completiondatetime = "null"
              if 'CompletionDateTime' in row['Status']:
                  completiondatetime = str(row['Status']['CompletionDateTime'])
              athena_metrics += ','.join([queryid,querydatabase,executiontime,datascanned,status,submissiondatetime,completiondatetime])+'\n'
      except Exception as e:
          print (e)

      sys.stdout=open("out" + str(i) + ".csv","w")
      print (athena_metrics)
      sys.stdout.close()
      
      s3 = boto3.resource('s3', region_name='us-west-2')

      now = datetime.datetime.now()
      currentyear = now.year
      currentmonth = now.month
      currentday = now.day
      infile = 'out' + str(i) + '.csv'
      outfile = 'athena-metrics'+'/'+str(currentyear)+'/'+str(currentmonth)+'/'+str(currentday)+'/'+'out' + str(i) + '.csv'
      s3.meta.client.upload_file(infile, destination_bucket, outfile)

      i = i + 1

main()
