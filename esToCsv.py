from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from ssl import create_default_context
from concurrent.futures import ThreadPoolExecutor
import json, ast, code, copy, os, urllib3, hashlib, csv, sys, argparse, multiprocessing, requests, logging, math, threading, time
from datetime import datetime, timedelta
import dateutil.parser as dateparser
from requests.auth import HTTPBasicAuth

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

def threads_finished():
  global threads_totals
  if threads_totals[0] != '100%':
    return False
  iterator = iter(threads_totals)
  try:
    first = next(iterator)
  except StopIteration:
    return True
  return all(first == rest for rest in iterator)
    

def thread_printer():
  global threads_totals
  time.sleep(1)
  while not threads_finished():
    time.sleep(0.5)
    status_bars = "[ "
    if len(threads_totals) == 1:
      status_bars += "Thread Main --> {}".format(threads_totals[0]) + " ]"
    else:
      for index, thread in enumerate(threads_totals):
        filler = " ]" if index == (len(threads_totals) - 1) else "  /  "
        status_bars += "Thread {} --> {}".format(index, threads_totals[index]) + filler
    print("\r" + status_bars, end='')

def fetch_arguments(): # TODO add function to check dates in iso8601 format
  ap = argparse.ArgumentParser()
  ap.add_argument("-ho", "--host", required=False, help="Elasticsearch host. If not set, localhost will be used", default="localhost")
  ap.add_argument("-f", "--fields", required=True, help="Elasticsearch fields, passed as a string with commas between fields and no whitespaces (e.g. \"field1,field2\")")
  ap.add_argument("-e", "--export_path", required=False, help="path where to store the csv file. If not set, 'es_export.csv' will be used", type=check_csv_valid_filename, default="es_export.csv")
  ap.add_argument("-sd", "--starting_date", required=False, help="query starting date", default='now-1000y') # TODO force iso format
  ap.add_argument("-ed", "--ending_date", required=False, help="query ending date", default='now+1000y') # TODO force iso format
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
  if (args['starting_date'] != 'now-1000y' or args['ending_date'] != 'now+1000y') and args['time_field'] == None:
    sys.exit("\nIf you set either a starting_date or an ending_date you have to set a --time_field to sort on, too.")
  
  if args['enable_multiprocessing'] and args['time_field'] == None:
    sys.exit("\nYou have to set a --time_field in order to use multiprocessing.")
  
  return True

def check_csv_already_written(filename):
  if os.path.exists(filename):
    overwrite_file = ''
    while overwrite_file not in ['y', 'n']:
      overwrite_file = input("\nThere is already a csv file at the given path ({}). Do you want to overwrite it? (y/n)".format(filename)).lower().strip()
    if overwrite_file == 'n':
      sys.exit("\nExiting script not to overwrite the file. No query has been run.")
  return filename

def build_es_query(args, starting_date, ending_date, order='asc', size=None, count_query=False):
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
  ES_QUERY = '{' + SIZE + SORT_QUERY + '"query":{"bool":{"must":[{"query_string":{"query":"' + QUERY_STRING + '"}}' + RANGE_QUERY + ']}}}'
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

def update_percentage(thread_name, processed_docs, total_hits):
  global threads_totals
  threads_totals_position = 0 if thread_name == 'Main' else thread_name
  partial_percentage = str(math.floor(processed_docs*100/total_hits)) + "%"
  threads_totals[threads_totals_position] = partial_percentage

def fetch_es_data(args, starting_date, ending_date, thread_name='Main'):
  logging.info("Thread {}: starts fetching data from {} to {}".format(thread_name, starting_date, ending_date))
  ES_INSTANCE = build_es_connection(args)
  ES_INDEX = args['index']
  SCROLL_TIMEOUT = args['scroll_timeout']
  BATCH_SIZE = args['batch_size']
  FIELDS_OF_INTEREST = args['fields'].split(',')
  ES_QUERY = build_es_query(args, starting_date, ending_date)
  ES_COUNT_QUERY = build_es_query(args, starting_date, ending_date, count_query=True)

  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  count_url = "http://" + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_count"
  total_hits = requests.get(count_url, data=ES_COUNT_QUERY, headers=headers, auth=HTTPBasicAuth(args['user'], args['password'])).json()['count']
  
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
    logging.critical("\nThread {}: something went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{}".format(thread_name, e))
  
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
    #print('\rThread {}: Scrolling...(page {})\n'.format(thread_name, page_counter), end = '')
    es_data = ES_INSTANCE.scroll(scroll_id=sid, scroll=SCROLL_TIMEOUT)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      processed_docs += 1
      fetched_data.append(obj_to_list(hit['_source'], FIELDS_OF_INTEREST))
      update_percentage(thread_name, processed_docs, total_hits)
    # Update the scroll ID
    sid = es_data['_scroll_id']
    # Get the number of results that returned in the last scroll
    scroll_size = len(es_data['hits']['hits'])
    page_counter += 1

  # logging.info("\nThread {}: {} documents have been fetched.".format(thread_name, processed_docs))
  return fetched_data 

def get_actual_dates(args, starting_date, ending_date):
  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  search_url = "http://" + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_search"
  if starting_date == 'now-1000y':
    sdate_query = build_es_query(args, starting_date, ending_date, 'asc', 1)
    r = requests.get(search_url, data=sdate_query, headers=headers, auth=HTTPBasicAuth(args['user'], args['password'])).json()
    starting_date = r['hits']['hits'][0]['_source'][args['time_field']]
  if ending_date == 'now+1000y':
    edate_query = build_es_query(args, starting_date, ending_date, 'desc', 1)
    r = requests.get(search_url, data=edate_query, headers=headers, auth=HTTPBasicAuth(args['user'], args['password'])).json()
    ending_date = r['hits']['hits'][0]['_source'][args['time_field']]
  return [starting_date, ending_date]

def make_time_intervals(args, threads, starting_date, ending_date):
  starting_date, ending_date = get_actual_dates(args, starting_date, ending_date)
  sdate_in_seconds = dateparser.parse(starting_date).timestamp()
  edate_in_seconds = dateparser.parse(ending_date).timestamp()
  interval_in_seconds = (edate_in_seconds - sdate_in_seconds) / threads
  dates_for_threads = [[], []]
  for thread in range(0, threads):
    sdate = datetime.fromtimestamp(sdate_in_seconds + thread*interval_in_seconds).isoformat()
    edate = datetime.fromtimestamp(sdate_in_seconds + (thread + 1)*interval_in_seconds).isoformat()
    dates_for_threads[0].append(sdate)
    dates_for_threads[1].append(edate)
  dates_for_threads[1][-1] = datetime.fromtimestamp(edate_in_seconds).isoformat()
  return dates_for_threads

def main():
  global threads_totals
  args = fetch_arguments()
  check_arguments_conflicts(args)

  logging.info("################ LAUNCHING THE WEB SCRIPT ################\n")

  EXPORT_PATH = check_csv_already_written(args['export_path'])

  if args['enable_multiprocessing']:
    logging.info('MULTITHREAD PROCESSING\n')
    threads_to_use = min(multiprocessing.cpu_count(), safe_toint_cast(args['threads'])) if args['threads'] != None else multiprocessing.cpu_count()
    threads_totals = (['0%' for i in range(threads_to_use)])
    lists_to_join = [[] for i in range(threads_to_use)]
    
    printing_thread = threading.Thread(target=thread_printer)
    printing_thread.start()
    
    # Split total time range in equals time intervals so to let each thread work on a partial set of data and store the resulting list as a sublist of the lists_to_join list
    threads_intervals = make_time_intervals(args, threads_to_use, args['starting_date'], args['ending_date'])

    # Build the list of arguments to pass to the function each thread will run
    thread_function_arguments = [[args for i in range(threads_to_use)], *threads_intervals, [i for i in range(threads_to_use)]]
    
    # Create and start threads_to_use number of threads
    with ThreadPoolExecutor(threads_to_use) as executor:
      [*lists_to_join] = executor.map(fetch_es_data, *thread_function_arguments)

    printing_thread.join()
    # Create the final list, concatenating the partial sublists resulting from the threads processing and removing each first element of each sublist starting from the second one since it would be equal to the final element of the previous sublist (range in es query use the gte and lte operators)
    list_to_write = lists_to_join.pop(0)
    for fetched_partial_list in lists_to_join:
      fetched_partial_list.pop(0)
      list_to_write += fetched_partial_list
  else:
    logging.info('SINGLE THREAD PROCESSING\n')
    threads_totals = ['0%']
    printing_thread = threading.Thread(target=thread_printer)
    printing_thread.start()
    
    list_to_write = fetch_es_data(args, args['starting_date'], args['ending_date'])
    printing_thread.join()
  
  logging.info('\nWriting the csv file...')
  with open(EXPORT_PATH, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(list_to_write)


if __name__ == "__main__":
  format = "%(asctime)s -- %(message)s"
  logging.getLogger("requests").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("concurrent").setLevel(logging.WARNING)
  logging.getLogger("asyncio").setLevel(logging.WARNING)
  logging.getLogger("elasticsearch").setLevel(logging.WARNING)
  logging.basicConfig(format=format, level=logging.INFO, datefmt="[%Y-%m-%dT%H:%M:%S]")

  threads_totals = []
  main()

