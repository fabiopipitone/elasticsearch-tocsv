from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from ssl import create_default_context
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import json, ast, code, copy, os, urllib3, hashlib, csv, sys, argparse, multiprocessing, requests, logging, math, threading, time
from datetime import datetime, timedelta
import dateutil.parser as dateparser
from dateutil import tz
import pytz
from requests.auth import HTTPBasicAuth
from tqdm import *
import getpass

# Disable warning about not using an ssl certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TqdmLoggingHandler(logging.Handler):
  def __init__(self, level=logging.NOTSET):
    super().__init__(level)

  def emit(self, record):
    try:
      msg = self.format(record)
      tqdm.write(msg)
      self.flush()
    except (KeyboardInterrupt, SystemExit):
      raise
    except:
      self.handleError(record)

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

def check_valid_date(date_string):
  if (date_string != "now-1000y" and date_string != "now+1000y"):
    try:
      dateparser.parse(date_string)
    except:
      sys.exit("\nThe date set ({}) is not valid".format(date_string))
  return True

def request_to_es(url, query, user='', pwd='', timeout=10):
  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  try:
    r = requests.get(url, data=query, headers=headers, auth=HTTPBasicAuth(user, pwd), timeout=10).json()
  except Exception as e:
    logging.error("\n\nSomething when wrong connecting to the ES instance. Check out the raised exception: \n\n{}".format(e))
    os._exit(os.EX_OK)
  return r

def valid_bound_dates(args):
  if args['starting_date'] != 'now-1000y' and args['ending_date'] != 'now+1000y':
    sdate = dateparser.parse(args['starting_date']).astimezone(args['timezone'])
    edate = dateparser.parse(args['ending_date']).astimezone(args['timezone'])
    return edate > sdate
  return True

def check_timezone_validity(timezone):
  if timezone is None:
    return tz.tzlocal()
  elif timezone in pytz.all_timezones:
    return tz.gettz(timezone)
  else:
    logging.error("\n\nSomething is wrong with the timezone you set {}. Please set a timezone included in the pytz.all_timezones or leave it blank to set the local timezone of this machine".format(timezone))
    os._exit(os.EX_OK)

def fetch_arguments():
  ap = argparse.ArgumentParser()
  ap.add_argument("-ho", "--host", required=False, help="Elasticsearch host. If not set, localhost will be used", default="localhost")
  ap.add_argument("-f", "--fields", required=True, help="Elasticsearch fields, passed as a string with commas between fields and no whitespaces (e.g. \"field1,field2\")")
  ap.add_argument("-e", "--export_path", required=False, help="path where to store the csv file. If not set, 'es_export.csv' will be used", type=check_csv_valid_filename, default="es_export.csv")
  ap.add_argument("-sd", "--starting_date", required=False, help="query starting date. Must be set in iso 8601 format, without the timezone that can be specified in the --timezone option (e.g. \"YYYY-MM-ddTHH:mm:ss\")", default='now-1000y')
  ap.add_argument("-ed", "--ending_date", required=False, help="query ending date. Must be set in iso 8601 format, without the timezone that can be specified in the --timezone option (e.g. \"YYYY-MM-ddTHH:mm:ss\")", default='now+1000y')
  ap.add_argument("-t", "--time_field", required=False, help="time field to query on. If not set and --starting_date or --ending_date are set and exception will be raised")
  ap.add_argument("-q", "--query_string", required=False, help="Elasticsearch query string. Put it between quotes and escape internal quotes characters (e.g. \"one_field: foo AND another_field.keyword: \\\"bar\\\"\"", default="*")
  ap.add_argument("-p", "--port", required=False, help="Elasticsearch port. If not set, the default port 9200 will be used", default=9200, type=int)
  ap.add_argument("-u", "--user", required=False, help="Elasticsearch user", default='')
  ap.add_argument("-pw", "--password", required=False, help="Elasticsearch password in clear. If set, the --secret_password will be ignored. If both this a --secret_password are not set, a prompt password will be asked anyway (leave it blank if not needed).")
  ap.add_argument("-spw", "--secret_password", required=False, help="env var pointing the Elasticsearch password. If both this a --password are not set, a prompt password will be asked anyway (leave it blank if not needed).")
  ap.add_argument("-s", "--ssl", required=False, help="require ssl connection. Default to False. Set to True to enable it", type=bool, default=False)
  ap.add_argument("-c", "--cert_verification", required=False, help="require ssl certificate verification. Default to False. Set to True to enable it", type=bool, default=False)
  ap.add_argument("-i", "--index", required=True, help="Elasticsearch index pattern to query on. To use wildcard (*) put the index in quotes (e.g. \"my-indices*\")")
  ap.add_argument("-b", "--batch_size", required=False, help="batch size for the scroll API. Default to 5000. Max 10000. Increasing it might impact the ES instance heap memory. If you want to set a value greater than 10000, you must set the max_result_window elasticsearch property accordingly first. Please check out the elasticsearch documentation before increasing that value on the specified index --> https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html", type=int, default=5000)
  ap.add_argument("-o", "--scroll_timeout", required=False, help="scroll window timeout. Default to 4m", default='4m')
  ap.add_argument("-pn", "--process_number", required=False, help="number of processes to run the script on. Default to max number of processes for the hosting machine", type=int)
  ap.add_argument("-em", "--enable_multiprocessing", required=False, help="enable the multiprocess options. Default to False. Set to True to exploit multiprocessing. If set to True a --time_field to sort on must be set or an exception will be raised", type=bool, default=False)
  ap.add_argument("-tz", "--timezone", required=False, help="timezone to set according to the time zones naming convention (e.g. \"America/New_York\" or \"Europe/Paris\" or \"UTC\"). If not set, the local timezone of the present machine will be used", default=None)
  return vars(ap.parse_args())

def check_arguments_conflicts(args):
  if (args['starting_date'] != 'now-1000y' or args['ending_date'] != 'now+1000y') and args['time_field'] == None:
    sys.exit("\nIf you set either a starting_date or an ending_date you have to set a --time_field to sort on, too.")
  
  if args['enable_multiprocessing'] and args['time_field'] == None:
    sys.exit("\nYou have to set a --time_field in order to use multiprocessing.")

  args['timezone'] = check_timezone_validity(args['timezone'])
  check_valid_date(args['starting_date'])
  check_valid_date(args['ending_date'])

  if not valid_bound_dates(args):
    sys.exit("\nThe --starting_date you set ({}) comes after the --ending_date ({}). Please set a valid time interval".format(args['starting_date'], args['ending_date']))

  return True

def final_pw(args):
  pw = args['password'] if args['password'] != None else os.environ[args['secret_password']] if args['secret_password'] != None and args['secret_password'] in os.environ else ''
  if pw == '':
    pw = getpass.getpass("Enter your es instance password. If not needed, press ENTER:  ")
  args['password'] = pw

def check_csv_already_written(filename):
  if os.path.exists(filename):
    overwrite_file = ''
    while overwrite_file not in ['y', 'n']:
      overwrite_file = input("\nThere is already a csv file at the given path ({}). Do you want to overwrite it? (y/n)".format(filename)).lower().strip()
    if overwrite_file == 'n':
      sys.exit("\nExiting script not to overwrite the file. No query has been run.")
  return filename

def build_source_query(fields_of_interest):
  return  '\"_source\":' + str(fields_of_interest.split(',')).replace("'",'"') + ','

def build_es_query(args, starting_date, ending_date, order='asc', size=None, count_query=False, source=None):
  QUERY_STRING = args['query_string'] #TODO chech how to properly escape internal quotes
  SIZE = ('"size": ' + str(size) + ',') if size != None else ''
  if args['time_field'] != None:
    TIME_FIELD = args['time_field']
    FROM_DATE = starting_date if starting_date != None else "now-1000y"
    TO_DATE = ending_date if ending_date != None else "now+1000y"
    SORT_QUERY = '"sort":[{"' + TIME_FIELD + '":{"order":"' + order + '"}}],' if not count_query else ''
    RANGE_QUERY = ",{\"range\":{\"" + TIME_FIELD + "\":{\"gte\":\"" + FROM_DATE + "\",\"lte\":\"" + TO_DATE + "\"}}}"
  else:
    SORT_QUERY = RANGE_QUERY = ''
  SOURCE_QUERY = build_source_query(source) if not source == None and not count_query else ''
  ES_QUERY = '{' + SOURCE_QUERY + SIZE + SORT_QUERY + '"query":{"bool":{"must":[{"query_string":{"query":"' + QUERY_STRING + '"}}' + RANGE_QUERY + ']}}}'
  return ES_QUERY

def build_es_connection(args):
  return Elasticsearch( hosts=[{'host': args['host'], 'port': args['port']}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(args['user'], args['password']),
                          use_ssl=args['ssl'],
                          verify_certs=args['cert_verification'],
                          retry_on_timeout=True,
                          timeout=50 )

def fetch_es_data(args, starting_date, ending_date, process_name='Main'):
  global pbars
  process_number = 0 if process_name == 'Main' else process_name
  log.info("Process {}: starts fetching data from {} to {}".format(process_name, starting_date, ending_date))
  ES_INSTANCE = build_es_connection(args)
  ES_INDEX = args['index']
  SCROLL_TIMEOUT = args['scroll_timeout']
  BATCH_SIZE = args['batch_size']
  FIELDS_OF_INTEREST = args['fields'].split(',')
  ES_QUERY = build_es_query(args, starting_date, ending_date, source=args['fields'])
  ES_COUNT_QUERY = build_es_query(args, starting_date, ending_date, count_query=True, source=args['fields'])

  count_url = "http://" + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_count"
  total_hits = request_to_es(count_url, ES_COUNT_QUERY, args['user'], args['password'])['count']
  pbar = tqdm(total=total_hits, position=process_number, leave=False, desc="Process {}".format(process_number), ncols=100)
  
  processed_docs = 0
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
    logging.error("\Process {}: something went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{}".format(process_name, e))
    os._exit(os.EX_OK)
  
  # Save parameters for scrolling
  sid = es_data['_scroll_id']
  scroll_size = len(es_data['hits']['hits'])
  page_counter = 1

  # Process current batch of hits before starting to scroll
  for hit in es_data['hits']['hits']:
    processed_docs += 1
    fetched_data.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))

  # Scroll and add hits to the fetched_data list
  while scroll_size > 0:
    es_data = ES_INSTANCE.scroll(scroll_id=sid, scroll=SCROLL_TIMEOUT)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      processed_docs += 1
      fetched_data.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))
      pbar.update(1)
    # Update the scroll ID
    sid = es_data['_scroll_id']
    # Get the number of results that returned in the last scroll
    scroll_size = len(es_data['hits']['hits'])
    page_counter += 1
  return fetched_data 

def get_actual_bound_dates(args, starting_date, ending_date):
  search_url = "http://" + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_search"
  if starting_date == 'now-1000y':
    sdate_query = build_es_query(args, starting_date, ending_date, 'asc', 1, source=args['time_field'])
    r = request_to_es(search_url, sdate_query, args['user'], args['password'])
    starting_date = r['hits']['hits'][0]['_source'][args['time_field']]
  if ending_date == 'now+1000y':
    edate_query = build_es_query(args, starting_date, ending_date, 'desc', 1, source=args['time_field'])
    r = request_to_es(search_url, edate_query, args['user'], args['password'])
    ending_date = r['hits']['hits'][0]['_source'][args['time_field']]
  starting_date = dateparser.parse(starting_date).astimezone(args['timezone']).isoformat()
  ending_date = dateparser.parse(ending_date).astimezone(args['timezone']).isoformat()
  return [starting_date, ending_date]

def make_time_intervals(args, processes, starting_date, ending_date):
  starting_date, ending_date = get_actual_bound_dates(args, starting_date, ending_date)
  sdate_in_seconds = dateparser.parse(starting_date).timestamp()
  edate_in_seconds = dateparser.parse(ending_date).timestamp()
  interval_in_seconds = (edate_in_seconds - sdate_in_seconds) / processes
  dates_for_processes = [[], []]
  for process in range(0, processes):
    sdate = datetime.fromtimestamp(sdate_in_seconds + process*interval_in_seconds).astimezone(args['timezone']).isoformat()
    edate = datetime.fromtimestamp(sdate_in_seconds + (process + 1)*interval_in_seconds).astimezone(args['timezone']).isoformat()
    dates_for_processes[0].append(sdate)
    dates_for_processes[1].append(edate)
  dates_for_processes[1][-1] = ending_date
  return dates_for_processes

def main():
  args = fetch_arguments()
  check_arguments_conflicts(args)
  final_pw(args)

  log.info("################ LAUNCHING THE ES_TO_CSV SCRIPT ################\n")

  EXPORT_PATH = check_csv_already_written(args['export_path'])

  if args['enable_multiprocessing']:
    log.info('MULTIPROCESSING ENABLED\n')
    processes_to_use = min(multiprocessing.cpu_count(), safe_toint_cast(args['process_number'])) if args['process_number'] != None else multiprocessing.cpu_count()
    lists_to_join = [[] for i in range(processes_to_use)]
    
    # Split total time range in equals time intervals so to let each process work on a partial set of data and store the resulting list as a sublist of the lists_to_join list
    processes_intervals = make_time_intervals(args, processes_to_use, args['starting_date'], args['ending_date'])

    # Build the list of arguments to pass to the function each process will run
    process_function_arguments = [[args for i in range(processes_to_use)], *processes_intervals, [i for i in range(processes_to_use)]]
    
    # Create and start processes_to_use number of processes
    with ProcessPoolExecutor(processes_to_use) as executor:
      [*lists_to_join] = executor.map(fetch_es_data, *process_function_arguments)

    # Create the final list, concatenating the partial sublists resulting from the processes processing and removing each first element of each sublist starting from the second one since it would be equal to the final element of the previous sublist (range in es query use the gte and lte operators)
    list_to_write = lists_to_join.pop(0)
    for fetched_partial_list in lists_to_join:
      fetched_partial_list.pop(0)
      list_to_write += fetched_partial_list
  else:
    log.info('SINGLE PROCESS RUN\n')
    list_to_write = fetch_es_data(args, args['starting_date'], args['ending_date'])
  
  log.info('Writing the csv file...')
  with open(EXPORT_PATH, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(list_to_write)


if __name__ == "__main__":
  formatter = logging.Formatter("%(asctime)s -- %(message)s")
  logging.getLogger("requests").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("concurrent").setLevel(logging.WARNING)
  logging.getLogger("asyncio").setLevel(logging.WARNING)
  logging.getLogger("elasticsearch").setLevel(logging.WARNING)

  log = logging.getLogger(__name__)
  log.setLevel(logging.INFO)
  handler = TqdmLoggingHandler()
  handler.setFormatter(formatter)
  log.addHandler(handler)

  pbars = []
  main()

