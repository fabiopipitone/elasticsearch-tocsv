from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from ssl import create_default_context
import json, ast, code, copy, os, urllib3, hashlib, csv, sys, argparse, multiprocessing, requests
from datetime import datetime, timedelta

# Disable warning about not using an ssl certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

############ UTILITY FUNCTIONS ############
def obj_to_list(object, fields):
  list_of_values = []
  for field in fields:
    list_of_values.append(object[field]) if field in object else list_of_values.append(None)
  return list_of_values

def check_csv_valid_filename(filename):
  if filename[-4:] != '.csv':
    raise argparse.ArgumentTypeError("{} is not a valid path where to store the retrieved data. It must be a csv file".format(filename))
  return filename

def fetch_arguments():
  ap = argparse.ArgumentParser()
  ap.add_argument("-ho", "--host", required=False, help="Elasticsearch host. If not set, localhost will be used", default="localhost")
  ap.add_argument("-f", "--fields", required=True, help="Elasticsearch fields, passed as a string with commas between fields and no whitespaces (e.g. \"field1,field2\")")
  ap.add_argument("-e", "--export_path", required=False, help="path where to store the csv file. If not set, 'es_export.csv' will be used", type=check_csv_valid_filename, default="es_export.csv")
  ap.add_argument("-sd", "--starting_date", required=False, help="query starting date")
  ap.add_argument("-ed", "--ending_date", required=False, help="query ending date")
  ap.add_argument("-t", "--time_field", required=False, help="time field to query on. If not set and --starting_date or --ending_date are set and exception will be raised")
  ap.add_argument("-q", "--query_string", required=False, help="Elasticsearch query string. Put it between quotes and escape internal quotes characters (e.g. \"one_field: foo AND another_field.keyword: \\\"bar\\\"\"", default="*")
  ap.add_argument("-p", "--port", required=False, help="Elasticsearch port. If not set, the default port 9200 will be used", default=9200, type=int)
  ap.add_argument("-u", "--user", required=False, help="Elasticsearch user", default='')
  ap.add_argument("-pw", "--password", required=False, help="Elasticsearch password in clear. If set, the --secret_password will be ignored")
  ap.add_argument("-spw", "--secret_password", required=False, help="env var pointing the Elasticsearch password")
  ap.add_argument("-s", "--ssl", required=False, help="require ssl connection. Default to False. Set to True to enable it", type=bool, default=False)
  ap.add_argument("-c", "--cert_verification", required=False, help="require ssl certificate verification. Default to False. Set to True to enable it", type=bool, default=False)
  ap.add_argument("-i", "--index", required=True, help="Elasticsearch index pattern to query on. To use wildcard (*) put the index in quotes (e.g. \"my-indices*\")")
  ap.add_argument("-b", "--batch_size", required=False, help="batch size for the scroll API. Default to 5000. Max 10000", type=int, default=5000)
  ap.add_argument("-o", "--scroll_timeout", required=False, help="scroll window timeout. Default to 4m", default='4m')
  ap.add_argument("-th", "--threads", required=False, help="number of threads to run the script on. Default to max number of thread for the hosting machine", type=int)
  ap.add_argument("-em", "--enable_multiprocessing", required=False, help="enable the multithread options. Default to False. Set to True to exploit multiprocessing. If set to True a --time_field to sort on must be set or an exception will be raised", type=bool, default=False)
  return vars(ap.parse_args())

def check_arguments_conflicts(args):
  if (args['starting_date'] != None or args['ending_date'] != None) and args['time_field'] == None:
    sys.exit("\nIf you set either a starting_date or an ending_date you have to set a --time_field to sort on, too.")
  
  if args['enable_multiprocessing'] and args['time_field'] == None:
    sys.exit("\nYou have to set a --time_field in order to use multiprocessing.")
  
  return True

def check_csv_already_written(filename):
  if os.path.exists(filename):
    overwrite_file = ''
    while overwrite_file not in ['y', 'n']:
      overwrite_file = input("\nThere is already a csv file at the given path ({}). Do you want to overwrite it? (y/n)".format(EXPORT_PATH)).lower().strip()
    if overwrite_file == 'n':
      sys.exit("\nExiting script not to overwrite the file. No query has been run.")
  return filename

def build_es_query(args, starting_date, ending_date):
  QUERY_STRING = args['query_string'] #TODO chech how to properly escape internal quotes
  if args['time_field'] != None:
    TIME_FIELD = args['time_field']
    FROM_DATE = starting_date if starting_date != None else "now-1000y"
    TO_DATE = ending_date if ending_date != None else "now+1000y"
    SORT_QUERY = '"sort":[{"' + TIME_FIELD + '":{"order":"asc"}}],'
    RANGE_QUERY = ",{\"range\":{\"" + TIME_FIELD + "\":{\"gte\":\"" + FROM_DATE + "\",\"lte\":\"" + TO_DATE + "\"}}}"
  else:
    SORT_QUERY = RANGE_QUERY = ''
  ES_QUERY = '{' + SORT_QUERY + '"query":{"bool":{"must":[{"query_string":{"query":"' + QUERY_STRING + '"}}' + RANGE_QUERY + ']}}}'
  return ES_QUERY

def build_es_connection(args):
  FROM_PW = args['password'] if args['password'] != None else os.environ[args['secret_password']] if args['secret_password'] != None and args['secret_password'] in os.environ else ''
  return Elasticsearch( hosts=[{'host': args['host'], 'port': args['port']}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(args['user'], FROM_PW),
                          use_ssl=args['ssl'],
                          verify_certs=args['cert_verification'],
                          retry_on_timeout=True,
                          timeout=50 )

def fetch_es_data(args):
  ES_INSTANCE = build_es_connection(args)
  ES_INDEX = args['index']
  SCROLL_TIMEOUT = args['scroll_timeout']
  BATCH_SIZE = args['batch_size']
  FIELDS_OF_INTEREST = args['fields'].split(',')
  ES_QUERY = build_es_query(args, args['starting_date'], args['ending_date'])

  total_hits = 0
  fetched_data = [FIELDS_OF_INTEREST]

  try:
    es_data = ES_INSTANCE.search(
      index = ES_INDEX,
      _source = FIELDS_OF_INTEREST,
      scroll = SCROLL_TIMEOUT,
      size = BATCH_SIZE,
      body = ES_QUERY
    )
  except Exception as e:
    print("\nSomething went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{}".format(e))
  
  # Save parameters for scrolling
  sid = es_data['_scroll_id']
  scroll_size = len(es_data['hits']['hits'])
  page_counter = 1

  # Process current batch of hits before starting to scroll
  for hit in es_data['hits']['hits']:
    total_hits += 1
    fetched_data.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))

  # Scroll and add hits to the fetched_data list
  while scroll_size > 0:
    print('\rScrolling...(page {})'.format(page_counter), end = '')
    es_data = ES_INSTANCE.scroll(scroll_id=sid, scroll=SCROLL_TIMEOUT)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      total_hits += 1
      fetched_data.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))
    # Update the scroll ID
    sid = es_data['_scroll_id']
    # Get the number of results that returned in the last scroll
    scroll_size = len(es_data['hits']['hits'])
    page_counter += 1

  return fetched_data  

def main():
  args = fetch_arguments()
  check_arguments_conflicts(args)

  print('################ LAUNCHING THE WEB SCRIPT -- DATETIME {} ################\n'.format(datetime.now().strftime('%d/%m/%Y_%H:%M:%S')))

  EXPORT_PATH = check_csv_already_written(args['export_path'])

  # ES_INSTANCE = build_es_connection(args)
  # ES_INDEX = args['index']
  # SCROLL_TIMEOUT = args['scroll_timeout']
  # BATCH_SIZE = args['batch_size']
  # FIELDS_OF_INTEREST = args['fields'].split(',')
  # ES_QUERY = build_es_query(args)

  if args['enable_multiprocessing']:
    # Do the multithread code
    TOTAL_THREADS = min(multiprocessing.cpu_count(), safe_toint_cast(args['threads'])) if args['threads'] != None else multiprocessing.cpu_count()
    print('MULTITHREAD PROCESSING\n')
  else:
    print('SINGLE THREAD PROCESSING\n')
    list_to_write = fetch_es_data(args)
  
  print('\nWriting the csv file...')
  with open(EXPORT_PATH, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(list_to_write)


if __name__ == "__main__":
  main()

