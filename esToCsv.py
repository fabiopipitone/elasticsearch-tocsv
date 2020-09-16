from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from ssl import create_default_context
import json, ast, code, copy, os, urllib3, hashlib, csv, sys, argparse, multiprocessing, requests
from datetime import datetime, timedelta

# Disable warning about not using an ssl certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

############### PRINT SCRIPT RUN ##############
print('################ LAUNCHING THE WEB SCRIPT -- DATETIME {} ################\n'.format(datetime.now().strftime('%d/%m/%Y_%H:%M:%S')))
################## END BLOCK ##################

############ UTILITY FUNCTIONS ############
def obj_to_list(object, fields):
  list_of_values = []
  for field in fields:
    list_of_values.append(object[field]) if field in object else list_of_values.append(None)
  return list_of_values

def safe_toint_cast(string, rollback):
  try: 
    return int(string)
  except:
    return rollback

########## END UTILITY FUNCTIONS ##########

############ SCRIPT ARGUMENTS AND OPTIONS ############
ap = argparse.ArgumentParser()
ap.add_argument("-ho", "--host", required=False, help="Elasticsearch host. If not set, localhost will be used")
ap.add_argument("-f", "--fields", required=True, help="Elasticsearch fields, passed as a string with commas between fields and no whitespaces (e.g. \"field1,field2\")")
ap.add_argument("-e", "--export_path", required=False, help="path where to store the csv file. If not set, 'es_export.csv' will be used")
ap.add_argument("-sd", "--starting_date", required=False, help="query starting date")
ap.add_argument("-ed", "--ending_date", required=False, help="query ending date")
ap.add_argument("-t", "--time_field", required=False, help="time field to query on. If not set and --starting_date or --ending_date are set, @timestamp will be used as time field")
ap.add_argument("-q", "--es_query", required=False, help="Elasticsearch query. If set, --starting_date, --ending_date and --time_field will be ignored. Remember to use quotes and escape internal ones (e.g. ... -q \"{\\\"query\":{\\\"match_all\\\":{}}}\") ")
ap.add_argument("-p", "--port", required=False, help="Elasticsearch port. If not set, the default port 9200 will be used")
ap.add_argument("-u", "--user", required=False, help="Elasticsearch user")
ap.add_argument("-pw", "--password", required=False, help="Elasticsearch password in clear. If set, the --secret_password will be ignored")
ap.add_argument("-spw", "--secret_password", required=False, help="env var pointing the Elasticsearch password")
ap.add_argument("-s", "--ssl", required=False, help="require ssl connection. Default to False. Set to True to enable it")
ap.add_argument("-c", "--cert_verification", required=False, help="require ssl certificate verification. Default to False. Set to True to enable it")
ap.add_argument("-i", "--index", required=True, help="Elasticsearch index pattern to query on. To use wildcard (*) put the index in quotes (e.g. \"my-indices*\")")
ap.add_argument("-b", "--batch_size", required=False, help="batch size for the scroll API. Default to 5000. Max 10000")
ap.add_argument("-o", "--scroll_timeout", required=False, help="scroll window timeout. Default to 4m")
ap.add_argument("-th", "--threads", required=False, help="number of threads to run the script on. Default to max number of thread for the hosting machine")
args = vars(ap.parse_args())
########## END SCRIPT ARGUMENTS AND OPTIONS ##########

############ GLOBAL VARIABLES ############
TOTAL_THREADS = min(multiprocessing.cpu_count(), safe_toint_cast(args['threads'])) if args['threads'] != None else multiprocessing.cpu_count()
FIELDS_OF_INTEREST = args['fields'].split(',')
FETCHED_DATA = [FIELDS_OF_INTEREST]
EXPORT_PATH = args['export_path'] if args['export_path'] != None else '/home/fabio/Desktop/esToCSV/exports/testing_export.csv'

if EXPORT_PATH[-4:] != '.csv':
  raise('Specified path is not a valid csv file')
elif os.path.exists(EXPORT_PATH):
  overwrite_file = ''
  while overwrite_file not in ['y', 'n']:
    overwrite_file = input("\nThere is already a csv file at the given path ({}). Do you want to overwrite it? (y/n)".format(EXPORT_PATH)).lower().strip()
  overwrite_file = True if overwrite_file == 'y' else False

if not overwrite_file:
  sys.exit("\nExiting script not to overwrite the file. No query has been run.")

if args['es_query'] != None: # TODO check query is legit
  ES_QUERY = args['es_query']
elif args['starting_date'] != None or args['ending_date'] != None:
  TIME_FIELD = args['time_field'] if args['time_field'] != None else '@timestamp'
  FROM_DATE = args['starting_date'] if args['starting_date'] != None else 'now-1000y'
  TO_DATE = args['ending_date'] if args['ending_date'] != None else 'now+1000y'
  ES_QUERY = '{"query":{"bool":{"must":[{"range":{"' + TIME_FIELD + '":{"gt":"' + FROM_DATE + '","lte":"' + TO_DATE + '"}}}]}}}'
else:
  ES_QUERY = '{"query":{"match_all":{}}}'

TOTAL_HITS = 0
########## END GLOBAL VARIABLES ##########

############ ES CONNECTION PARAMS ############

FROM_USERNAME = args['user'] if args['user'] != None else ''
FROM_PW = args['password'] if args['password'] != None else os.environ[args['secret_password']] if args['secret_password'] != None and args['secret_password'] in os.environ else ''
FROM_SSL = args['ssl'] if args['ssl'] != None else False
FROM_CERT_VERIFICATION = args['cert_verification'] if args['cert_verification'] != None else False
FROM_HOST = args['host'] if args['host'] != None else 'localhost'
FROM_PORT = safe_toint_cast(args['port'], 9200) if args['port'] != None else 9200

FROM_ES = Elasticsearch( hosts=[{'host': FROM_HOST, 'port': FROM_PORT}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(FROM_USERNAME, FROM_PW),
                          use_ssl=FROM_SSL,
                          verify_certs=FROM_CERT_VERIFICATION,
                          retry_on_timeout=True,
                          timeout=50 )

FROM_INDEX = args['index']
FROM_SCROLL_TIMEOUT = args['scroll_timeout'] if args['scroll_timeout'] != None else '4m'
FROM_SOURCE = FIELDS_OF_INTEREST
FROM_BODY = ES_QUERY
FROM_BATCH_SIZE = safe_toint_cast(args['batch_size'], 5000) if args['batch_size'] != None else 5000
################## END BLOCK ##################


################ RETRIEVE DATA ################
# Connect to ES server
try:
  my_data = FROM_ES.search(
    index = FROM_INDEX,
    _source = FROM_SOURCE,
    scroll = FROM_SCROLL_TIMEOUT,
    size = FROM_BATCH_SIZE,
    body = FROM_BODY
  )
except Exception as e:
  print("\nSomething went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{}".format(e))

sid = my_data['_scroll_id']
scroll_size = len(my_data['hits']['hits'])
page_counter = 1

# Process current batch of hits before starting to scroll
for hit in my_data['hits']['hits']:
  TOTAL_HITS += 1
  FETCHED_DATA.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))

# Scroll and add hits to the FETCHED_DATA list
while scroll_size > 0:
  print('\rScrolling...(page {})'.format(page_counter), end = '')
  my_data = FROM_ES.scroll(scroll_id=sid, scroll=FROM_SCROLL_TIMEOUT)
  # Process current batch of hits
  for hit in my_data['hits']['hits']:
    TOTAL_HITS += 1
    FETCHED_DATA.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))
  # Update the scroll ID
  sid = my_data['_scroll_id']
  # Get the number of results that returned in the last scroll
  scroll_size = len(my_data['hits']['hits'])
  page_counter += 1

print("\n\n" + str(TOTAL_HITS) + ' documents have been fetched and processed. FETCHED_DATA list has been created.')

# Writing the csv file
print('\nWriting the csv file from the DataFrame...')
with open(EXPORT_PATH, 'w', newline='') as csvfile:
  writer = csv.writer(csvfile)
  writer.writerows(FETCHED_DATA)
