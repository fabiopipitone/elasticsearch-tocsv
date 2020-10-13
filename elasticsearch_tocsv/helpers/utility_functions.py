import os, logging, getpass, sys
import dateutil.parser as dateparser
import pandas as pd
from .color_wrappers import *

def build_source_query(fields_of_interest):
  return  '\"_source\":' + str(fields_of_interest).replace("'",'"') + ','

def build_es_query(args, starting_date, ending_date, order='asc', size=None, count_query=False, source=[]):
  QUERY_STRING = args['query_string'] #TODO chech how to properly escape internal quotes
  SIZE = ('"size": ' + str(size) + ',') if size != None and not count_query else ''
  if args['time_field'] != None:
    TIME_FIELD = args['time_field']
    SORT_QUERY = '"sort":[{"' + TIME_FIELD + '":{"order":"' + order + '"}}],' if not count_query else ''
    RANGE_QUERY = ",{\"range\":{\"" + TIME_FIELD + "\":{\"gte\":\"" + starting_date + "\",\"lte\":\"" + ending_date + "\"}}}"
  else:
    SORT_QUERY = RANGE_QUERY = ''
  SOURCE_QUERY = build_source_query(source) if not source == [] and not count_query else ''
  ES_QUERY = '{' + SOURCE_QUERY + SIZE + SORT_QUERY + '"query":{"bool":{"must":[{"query_string":{"query":"' + QUERY_STRING + '"}}' + RANGE_QUERY + ']}}}'
  return ES_QUERY

def add_meta_fields(obj, meta_fields, log=logging):
  try:
    for index, mf in enumerate(meta_fields):
      obj['_source'][mf] = obj[mf]
    return obj['_source']
  except Exception as e:
    log.critical(wrap_red(f"Something is wrong with the metadata retrieval in the following document {obj}. Here's the exception:\n\n{e}"))
    os._exit(os.EX_OK)

def nf_in_object(nested_field_name, obj):
  for name_part in nested_field_name:
    if name_part not in obj: return False
    obj = obj[name_part]
  return obj

def clean_object(obj, nested_fields):
  for nf in nested_fields:
    obj.pop(nf[0], None)
  return obj

def denest(fields, obj):
  nested_fields = [field.split('.') for field in fields if '.' in field]
  for nested_field in nested_fields:
    nf_value = nf_in_object(nested_field, obj)
    if nf_value: obj['.'.join(nested_field)] = nf_value
  obj = clean_object(obj, nested_fields)
  return obj

def final_pw(args, log):
  pw = args['password'] if args['password'] != None else os.environ[args['secret_password']] if args['secret_password'] != None and args['secret_password'] in os.environ else ''
  if pw == '':
    pw = (getpass.getpass("Enter your es instance password. If not needed, press ENTER:  ")).strip()
  if args['user'] == '' and pw != '': log.warning(wrap_orange('You set a password but not a user. You either forgot to set the user or you set a useless password. If something goes wrong with the authentication, this warning might ring a bell :)'))
  return pw

def add_timezone(date_string, timezone):
  try:
    return dateparser.parse(date_string).astimezone(timezone).isoformat()
  except:
    sys.exit(wrap_red(f"The date you set ({date_string}) (either --starting_date or --ending_date) is not in the valid iso8601 format (YYYY-MM-ddTHH:mm:ss) and the dateparser raised an exception. Please use the standard iso8601 format"))

def remove_duplicates(args, df):
  log = args['log']
  try: 
    if args['remove_duplicates']:
      log.info(wrap_blue('Removing possible duplicates from the dataframe'))
      df.drop_duplicates(subset=args['fields_to_export'], inplace=True)
    else:
      df.drop_duplicates(subset=['_id', *args['fields_to_export']], inplace=True)
    return df
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when removing duplicates (set by user or the possible duplicates due to multiprocessing). The partial csv files won't be deleted. Here's the exception: \n\n{e}"))

def aggregate_fields(filename, fields_as_string, agg_type):
  try:
    df = pd.read_csv(filename)
    aggregation_fields = fields_as_string.split(',')
    for field in df.columns.values.tolist():
      if field not in aggregation_fields: df.drop(columns=[field], inplace=True)
    df['estocsv_count'] = 1
    return df.groupby(aggregation_fields).agg(agg_type)
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when trying to aggregate the raw documents on the fields passed {fields_as_string}. Here's the exception: \n\n{e}"))