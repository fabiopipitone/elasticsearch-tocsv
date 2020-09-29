import os, logging, getpass, sys
import dateutil.parser as dateparser
import pandas as pd

def build_source_query(fields_of_interest):
  return  '\"_source\":' + str(fields_of_interest).replace("'",'"') + ','

def build_es_query(args, starting_date, ending_date, order='asc', size=None, count_query=False, source=[]):
  QUERY_STRING = args['query_string'] #TODO chech how to properly escape internal quotes
  SIZE = ('"size": ' + str(size) + ',') if size != None else ''
  if args['time_field'] != None:
    TIME_FIELD = args['time_field']
    SORT_QUERY = '"sort":[{"' + TIME_FIELD + '":{"order":"' + order + '"}}],' if not count_query else ''
    RANGE_QUERY = ",{\"range\":{\"" + TIME_FIELD + "\":{\"gte\":\"" + starting_date + "\",\"lte\":\"" + ending_date + "\"}}}"
  else:
    SORT_QUERY = RANGE_QUERY = ''
  SOURCE_QUERY = build_source_query(source) if not source == [] and not count_query else ''
  ES_QUERY = '{' + SOURCE_QUERY + SIZE + SORT_QUERY + '"query":{"bool":{"must":[{"query_string":{"query":"' + QUERY_STRING + '"}}' + RANGE_QUERY + ']}}}'
  return ES_QUERY

def add_meta_fields(obj, meta_fields):
  try:
    for index, mf in enumerate(meta_fields):
      obj['_source'][mf] = obj[mf]
    return obj['_source']
  except Exception as e:
    logging.critical("Something is wrong with the metadata retrieval in the following document {}. Here's the exception:\n\n{}".format(obj, e))
    os._exit(os.EX_OK)

def final_pw(args):
  pw = args['password'] if args['password'] != None else os.environ[args['secret_password']] if args['secret_password'] != None and args['secret_password'] in os.environ else ''
  if pw == '':
    pw = getpass.getpass("Enter your es instance password. If not needed, press ENTER:  ")
  return pw

def add_timezone(date_string, timezone):
  try:
    return dateparser.parse(date_string).astimezone(timezone).isoformat()
  except:
    sys.exit("Either the --starting_date ({}) or the --ending_date ({}) you set are not in the valid iso8601 format (YYYY-MM-ddTHH:mm:ss) and the dateparser raised an exception. Please use the standard iso8601 format")

def remove_duplicates(args, df, log):
  try: 
    if args['remove_duplicates']:
      log.info('Removing possible duplicates from the dataframe')
      df.drop_duplicates(subset=args['fields_to_export'], inplace=True)
    else:
      df.drop_duplicates(subset=['_id', *args['fields_to_export']], inplace=True)
    return df
  except Exception as e:
    sys.exit("Something went wrong when removing duplicates (set by user or the possible duplicates due to multiprocessing). The partial csv files won't be deleted. Here's the exception: \n\n{}".format(e))