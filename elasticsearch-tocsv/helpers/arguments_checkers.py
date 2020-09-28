import argparse, sys, re, os, logging
import dateutil.parser as dateparser
from .utility_functions import *
from .connection_tools import *
from dateutil import tz
import pytz

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

  if args['load_balance_interval'] != None: 
    args['load_balance_interval'] = parse_lbi(args['load_balance_interval'], args['allow_short_interval'], args['enable_multiprocessing']) 
    check_valid_lbi(args['starting_date'], args['ending_date'], args['load_balance_interval'])

  args['password'] = final_pw(args)

  if not valid_bound_dates(args):
    sys.exit("\nThe --starting_date you set ({}) comes after the --ending_date ({}). Please set a valid time interval".format(args['starting_date'], args['ending_date']))

  return args