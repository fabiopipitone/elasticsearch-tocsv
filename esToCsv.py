from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from ssl import create_default_context
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import json, ast, code, copy, os, urllib3, hashlib, csv, sys, argparse, multiprocessing, requests, logging, math, threading, time, glob, re, random
from datetime import datetime, timedelta
import dateutil.parser as dateparser
from dateutil import tz
import pandas as pd
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
  ap.add_argument("-mf", "--metadata_fields", required=False, help="Elasticsearch metadata fields (_index, _type, _id, _score), passed as a string with commas between fields and no whitespaces (e.g. \"_id,_index\")", default='')
  ap.add_argument("-e", "--export_path", required=False, help="path where to store the csv file. If not set, 'es_export.csv' will be used. Make sure the user who's launching the script is allowed to write to that path. WARNING: At the end of the process, unless --keep_partial is set to True, all the files with filenames \"[--export_path]_process*.csv\" will be remove. Make sure you're setting a --export_path which won't accidentally delete any other file apart from the ones created by this script", type=check_csv_valid_filename, default="es_export.csv")
  ap.add_argument("-lbi", "--load_balance_interval", required=False, help="set this option to build process intervals by events count rather than equally spaced over time. The shorter the interval, the better the events-to-process division, the higher the heavier the computation. It cannot go below 1d if --allow_short_interval is not set. Allowed values are a number plus one of the following [m, h, d, M, y], like 1d for 1 day or 4M for 4 months. Multiprocessing must be enabled to set this option", default=None)
  ap.add_argument("-asi", "--allow_short_interval", required=False, help="set this option to True to allow the --load_balance_interval to go below 1 day. With this option enabled the --load_balance_interval can be set up to 1 minute (1m)", default=False)
  ap.add_argument("-k", "--keep_partials", required=False, help="during the processing, various partial csv files will be created before joining them into a single csv. Set this flas to True if you want to keep also these partial files. Default to False. Notice the partial files will be kept anyway if something goes wrong during the creation of the final file.", type=bool, default=False)
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
  ap.add_argument("-rd", "--remove_duplicates", required=False, help="set to True to remove all duplicated events. Default to False. WARNING: two events with the same values of the fields specified in --fields will be considered duplicated and then unified even if on ES they might not be equal because of other fields not included in --fields. Check out the --metadata_fields option to include further info like the ES _id", default=False)
  ap.add_argument("-dp", "--disable_progressbar", required=False, help="turn off the progressbar visualization useful to keep track of fetching data progresses of various processes. Default to False. Set to True to simply be noticed when processes have done fetching data, without the loading progressbar.", type=bool, default=False)
  return vars(ap.parse_args())

def check_meta_fields(meta_fields_str):
  try:
    if meta_fields_str == '': return []

    meta_fields = meta_fields_str.split(',')
    for mf in meta_fields:
      if mf not in ['_index', '_type', '_id', '_score']:
        sys.exit("One of your --metadata_fields {} is not allowed. Allowed metadata fields are [_index, _type, _doc, _score]. Check out the --help to know how to set them.".format(mf))
    return meta_fields
  except Exception as e:
    sys.exit("Something is wrong with the --metadata_fields you set or how you set them. Check out the --help to know how to set them. Here's the exception:\n\n{}".format(e))

def check_fields(fields_str):
  try:
    fields = fields_str.split(',')
    return fields
  except Exception as e:
    sys.exit("Something is wrong with the --fields you set. Check out the --help to know how to set them. Here's the exception:\n\n{}".format(e))


def add_meta_fields(obj, meta_fields):
  try:
    for index, mf in enumerate(meta_fields):
      obj['_source'][mf] = obj[mf]
    return obj['_source']
  except Exception as e:
    log.critical("Something is wrong with the metadata retrieval in the following document {}. Here's the exception:\n\n{}".format(obj, e))
    os._exit(os.EX_OK)

def parse_lbi(lbi, allow_short_interval, multiprocess_enabled):
  try:
    number = re.search("\d+", lbi).group() if re.search("\d+", lbi) != None else None
    unit = re.search("[^\d]+", lbi).group() if re.search("[^\d]+", lbi) != None else None
    allowed_units = ['m', 'h', 'd', 'M', 'y'] if allow_short_interval else ['d', 'M', 'y']
    unit_in_seconds = {'m':60, 'h':3600, 'd':86400, 'M':2592000, 'y':31104000}
    if number == None or not number.isnumeric():
      sys.exit("--load_balance_interval option must begin with a number. Please check the --help to know how to properly set it")
    elif unit == None or unit == '' or unit not in allowed_units:
      sys.exit("--load_balance_interval unit must be one of the following {}. Please check the --help to know how to properly set it".format(str(allowed_units)))
    elif not multiprocess_enabled:
      sys.exit("Multiprocessing must be enabled (-em True) in order to set the --load_balance_interval")
    else:
      return int(number) * unit_in_seconds[unit]
  except:
    sys.exit("Something in the combination of --load_balance_interval and --allow_short_interval you set is wrong. Please check the --help to know how to properly set them")

def check_valid_lbi(starting_date, ending_date, lbi):
  sdate_in_seconds = dateparser.parse(starting_date).timestamp()
  edate_in_seconds = dateparser.parse(ending_date).timestamp()
  if lbi >= (edate_in_seconds - sdate_in_seconds):
    sys.exit("You set a --load_balance_interval greater than the timestamp --starting_date - --ending_date. You might as well avoid the multiprocessing :)")
  return True

def check_arguments_conflicts(args):
  if (args['starting_date'] != 'now-1000y' or args['ending_date'] != 'now+1000y') and args['time_field'] == None:
    sys.exit("\nIf you set either a starting_date or an ending_date you have to set a --time_field to sort on, too.")
  
  if args['enable_multiprocessing'] and args['time_field'] == None:
    sys.exit("\nYou have to set a --time_field in order to use multiprocessing.")

  args['url_prefix'] = 'https' if args['ssl'] else 'http'
  args['count_url'] = "{}://".format(args['url_prefix']) + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_count"

  args['timezone'] = check_timezone_validity(args['timezone'])
  check_valid_date(args['starting_date'])
  check_valid_date(args['ending_date'])
  args['starting_date'], args['ending_date'] = get_actual_bound_dates(args, args['starting_date'], args['ending_date'])

  args['fields'] = check_fields(args['fields'])

  args['metadata_fields'] = check_meta_fields(args['metadata_fields'])
  args['fields_to_export'] = args['metadata_fields'] + args['fields']

  args['load_balance_interval'] = parse_lbi(args['load_balance_interval'], args['allow_short_interval'], args['enable_multiprocessing'])
  check_valid_lbi(args['starting_date'], args['ending_date'], args['load_balance_interval'])

  if not valid_bound_dates(args):
    sys.exit("\nThe --starting_date you set ({}) comes after the --ending_date ({}). Please set a valid time interval".format(args['starting_date'], args['ending_date']))

  return True

def test_es_connection(args):
  try:
    url = "{}://{}:{}".format(args['url_prefix'], args['host'], args['port'])
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(args['user'], args['password']), timeout=10)
    if r.status_code != 200: sys.exit("Status code when trying to connect to your host at {} is not 200. Check out the reason here:\n\n{}".format(url, json.dumps(r.json(), indent=2)))
  except Exception as e:
    sys.exit("Something went wrong when testing the connection to your host. Check your host, port and credentials. Here's the exception:\n\n{}".format(e))

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
  return  '\"_source\":' + str(fields_of_interest).replace("'",'"') + ','

def build_es_query(args, starting_date, ending_date, order='asc', size=None, count_query=False, source=[]):
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
  SOURCE_QUERY = build_source_query(source) if not source == [] and not count_query else ''
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
  process_number = 0 if process_name == 'Main' else process_name
  process_tmp_subset = 1
  csv_partial_filename = "{}_process{}_{}.csv".format(args['export_path'][:-4], process_number, str(process_tmp_subset).zfill(5))
  log.info("Process {}: starts fetching data from {} to {}".format(process_name, starting_date, ending_date))
  ES_INSTANCE = build_es_connection(args)
  ES_INDEX = args['index']
  SCROLL_TIMEOUT = args['scroll_timeout']
  BATCH_SIZE = args['batch_size']
  DF_HEADER = ['_id'] + args['fields_to_export'] if not '_id' in args['fields_to_export'] else args['fields_to_export']
  META_FOR_EXTRACTION = ['_id'] + args['metadata_fields'] if not '_id' in args['metadata_fields'] else args['metadata_fields']
  ES_QUERY = build_es_query(args, starting_date, ending_date, source=args['fields'])
  ES_COUNT_QUERY = build_es_query(args, starting_date, ending_date, count_query=True)

  total_hits = request_to_es(args['count_url'], ES_COUNT_QUERY, args['user'], args['password'])['count']
  pbar = tqdm(total=total_hits, position=process_number, leave=False, desc="Process {} - Fetching".format(process_name), ncols=150) if not args['disable_progressbar'] else None
  
  fetched_data = []

  try:
    es_data = ES_INSTANCE.search(
      index = ES_INDEX,
      _source = args['fields'],
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

  # Process current batch of hits before starting to scroll
  for hit in es_data['hits']['hits']:
    fetched_data.append(add_meta_fields(hit, META_FOR_EXTRACTION)) 

  # Scroll and add hits to the fetched_data list
  while scroll_size > 0:
    es_data = ES_INSTANCE.scroll(scroll_id=sid, scroll=SCROLL_TIMEOUT)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      fetched_data.append(add_meta_fields(hit, META_FOR_EXTRACTION)) 
      if not args['disable_progressbar']: pbar.update(1)
    # Update the scroll ID
    sid = es_data['_scroll_id']
    # Get the number of results that returned in the last scroll
    scroll_size = len(es_data['hits']['hits'])

    # If this process has already fetched 10M events, create the df, write the partial csv and empty the fetched_data list
    if len(fetched_data) >= 10000000:
        write_csv(csv_partial_filename, DF_HEADER, exception_message="Something went wrong when trying to write the partial csv {}.".format(csv_partial_filename), list_to_convert=fetched_data)
        process_tmp_subset += 1
        csv_partial_filename = "{}_process{}_{}.csv".format(args['export_path'][:-4], process_number, str(process_tmp_subset).zfill(5))
        fetched_data = []
    
  write_csv(csv_partial_filename, DF_HEADER, exception_message="Something went wrong when trying to write the partial csv {}.".format(csv_partial_filename), list_to_convert=fetched_data)
  log.info("Process {} has fetched and processed {} docs. They've been split into {} partial csv file(s)".format(process_name, total_hits, process_tmp_subset))
  return True

def remove_duplicates(args, df):
  try: 
    if args['remove_duplicates']:
      log.info('Removing possible duplicates from the dataframe...')
      df.drop_duplicates(subset=args['fields_to_export'], inplace=True)
    else:
      df.drop_duplicates(subset=['_id', *args['fields_to_export']], inplace=True)
    return df
  except Exception as e:
    sys.exit("Something went wrong when removing duplicates (set by user or the possible duplicates due to multiprocessing). The partial csv files won't be deleted. Here's the exception: \n\n{}".format(e))

def write_csv(export_path, fields_to_export, exception_message='', df=None, list_to_convert=False):
  df = pd.DataFrame(list_to_convert, columns=fields_to_export) if list_to_convert else df
  try: 
    df.to_csv(export_path, index=False, columns=fields_to_export)
  except Exception as e:
    sys.exit("{} Here's the exception: \n\n{}".format(exception_message, e))

def join_partial_csvs(filename):
  try: 
    log.info('Joining partial csv files\n')
    list_of_csvs = sorted([csv for csv in glob.glob("{}_process*.csv".format(filename))])
    final_df = pd.concat([pd.read_csv(csv) for csv in list_of_csvs])
    return final_df
  except Exception as e:
    sys.exit("Something went wrong when trying to merge partial csv files previously created. The partial csv files won't be deleted. Here's the exception: \n\n{}".format(e))

def delete_partial_csvs(basic_filename):
  try: 
    list_of_csvs = sorted([csv for csv in glob.glob("{}_process*.csv".format(basic_filename))])
    for csv in list_of_csvs:
      os.remove(csv)
    return True
  except Exception as e:
    sys.exit("Something went wrong when trying to delete partial csv files previously created. Here's the exception: \n\n{}".format(e))

def add_timezone(date_string, timezone):
  try:
    return dateparser.parse(date_string).astimezone(timezone).isoformat()
  except:
    sys.exit("Either the --starting_date ({}) or the --ending_date ({}) you set are not in the valid iso8601 format (YYYY-MM-ddTHH:mm:ss) and the dateparser raised an exception. Please use the standard iso8601 format")

def get_actual_bound_dates(args, starting_date, ending_date):
  search_url = "{}://".format(args['url_prefix']) + args['host'] + ":" + str(args['port']) + "/" + args['index'] + "/_search"
  timezone = args['timezone']
  starting_date = add_timezone(starting_date, timezone) if not starting_date == "now-1000y" else starting_date
  ending_date = add_timezone(ending_date, timezone) if not ending_date == "now+1000y" else ending_date
  # Fetch date of first element from the specified starting_date
  sdate_query = build_es_query(args, starting_date, ending_date, 'asc', 1, source=args['time_field'].split())
  r = request_to_es(search_url, sdate_query, args['user'], args['password'])
  starting_date = add_timezone(r['hits']['hits'][0]['_source'][args['time_field']], timezone)
  # Fetch date of last element before the specified ending_date
  edate_query = build_es_query(args, starting_date, ending_date, 'desc', 1, source=args['time_field'].split())
  r = request_to_es(search_url, edate_query, args['user'], args['password'])
  ending_date = add_timezone(r['hits']['hits'][0]['_source'][args['time_field']], timezone)
  # Return real starting_date and ending_date with proper timezone
  return [starting_date, ending_date]

def make_intervals_by_load(args, processes, starting_date, ending_date):
  starting_date, ending_date = get_actual_bound_dates(args, starting_date, ending_date)
  total_count_query = build_es_query(args, starting_date, ending_date, count_query=True)
  total_hits = request_to_es(args['count_url'], total_count_query, args['user'], args['password'])['count']
  sdate_in_seconds = dateparser.parse(starting_date).timestamp()
  edate_in_seconds = dateparser.parse(ending_date).timestamp()
  dates_for_processes = [[], []]
  sdate = starting_date
  multiplier = 1
  edate = datetime.fromtimestamp(sdate_in_seconds + multiplier * args['load_balance_interval']).astimezone(args['timezone']).isoformat()
  pbar = tqdm(total=total_hits, position=1, leave=False, desc="Building load_weighted time intervals", ncols=150) if not args['disable_progressbar'] else None
  if args['disable_progressbar']: log.info("Building load_weighted time intervals. This operation might take a while depending on the ration total_hits_counted/load_balance_interval...")
  while len(dates_for_processes[0]) < processes:
    partial_count_query = build_es_query(args, sdate, edate, count_query=True) 
    hits = request_to_es(args['count_url'], partial_count_query, args['user'], args['password'])['count']
    if not args['disable_progressbar']: pbar.update(hits)
    if hits >= total_hits/processes: 
      multiplier = 1
      dates_for_processes[0].append(sdate) 
      dates_for_processes[1].append(edate)
      sdate = edate
      sdate_in_seconds = dateparser.parse(sdate).timestamp()
      edate = datetime.fromtimestamp(sdate_in_seconds + multiplier * args['load_balance_interval']).astimezone(args['timezone']).isoformat()
    else: 
      multiplier += 1
      edate = datetime.fromtimestamp(sdate_in_seconds + multiplier * args['load_balance_interval']).astimezone(args['timezone']).isoformat()
    if dateparser.parse(edate) > dateparser.parse(ending_date):
      final_intervals = make_time_intervals(args, processes - len(dates_for_processes[0]), sdate, ending_date)
      return [dates_for_processes[i] + final_intervals[i] for i in range(2)]
    if len(dates_for_processes[0]) == processes - 1:
      dates_for_processes[0].append(sdate)
      dates_for_processes[0].append(ending_date)
  return dates_for_processes


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
  test_es_connection(args)

  log.info("################ LAUNCHING THE ES_TO_CSV SCRIPT ################\n")

  EXPORT_PATH = check_csv_already_written(args['export_path'])

  if args['enable_multiprocessing']:
    log.info('CONNECTION TO ES HOST ESTABLISHED -- MULTIPROCESSING ENABLED\n')
    processes_to_use = min(multiprocessing.cpu_count(), safe_toint_cast(args['process_number'])) if args['process_number'] != None else multiprocessing.cpu_count()
    
    # Split total time range in equals time intervals so to let each process work on a partial set of data and store the resulting list as a sublist of the lists_to_join list
    processes_intervals = make_time_intervals(args, processes_to_use, args['starting_date'], args['ending_date']) if args['load_balance_interval'] == None else make_intervals_by_load(args, processes_to_use, args['starting_date'], args['ending_date'])

    # Build the list of arguments to pass to the function each process will run
    process_function_arguments = [[args for i in range(processes_to_use)], *processes_intervals, [i for i in range(processes_to_use)]]
    
    processes_done = [None for i in range(processes_to_use)]

    # Create and start processes_to_use number of processes
    with ProcessPoolExecutor(processes_to_use) as executor:
      processes_done = executor.map(fetch_es_data, *process_function_arguments)
  else:
    log.info('CONNECTION TO ES HOST ESTABLISHED -- SINGLE PROCESS RUN\n')
    fetch_es_data(args, args['starting_date'], args['ending_date'])

  while list(set(processes_done)) != [True]:
    continue

  # Joining partial csv files previously created into a single one
  final_df = join_partial_csvs(args['export_path'][:-4])
  
  # Remove duplicates
  final_df = remove_duplicates(args, final_df)

  # Write the CSV from the dataframe
  log.info('Exporting Pandas final DataFrame as {}...'.format(args['export_path']))
  write_csv(args['export_path'], args['fields_to_export'], "Something went wrong when trying to write the final csv after the merge of the partial csv files. The partial csv files won't be deleted.", df=final_df)

  # Delete partial csvs
  if not args['keep_partials']: delete_partial_csvs(args['export_path'][:-4])

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

  main()

