import os, logging, getpass, sys
import dateutil.parser as dateparser
import pandas as pd
import numpy as np
from dateutil import tz
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

def clean_dictionary(dictionaries, main_keys, dict_type, filename, log):
  dictionary = dictionaries[dict_type]
  if dict_type == '--rename_aggregations':
    aggregation_dict = dictionaries['--aggregation_types']
    for agg_key in aggregation_dict.keys():
      if agg_key not in dictionary.keys(): dictionary[agg_key] = f"estocsv__{agg_key}__{aggregation_dict[agg_key]}"
  for key in list(dictionary):
    if key not in main_keys: 
      log.warning(wrap_orange(f"\"{key}\" you set in your {dict_type} is not present among the columns of the raw data file \"{filename}\". It'll be removed from the aggregated column"))
      dictionary.pop(key, None)
  return dictionary

def clean_list(list_to_clean, main_keys):
  return [element for element in list_to_clean if element in main_keys]

def rename_df_columns(df, new_columns):
  return df.rename(columns=new_columns)

def aggregate_fields(filename, args, log, df_ready=None):
  try:
    df = pd.read_csv(filename) if df_ready is None else df_ready
    columns_to_keep = []
    aggregation_types = clean_dictionary({"--aggregation_types": args['aggregation_types']}, df.columns, '--aggregation_types', filename, log)
    aggregation_names = clean_dictionary({"--aggregation_types": args['aggregation_types'], "--rename_aggregations": args['rename_aggregations']}, df.columns, '--rename_aggregations', filename, log)
    boolean_occurrences = clean_list(args['count_boolean_occurrences'].split(','), df.columns)
    aggregation_fields = [ column for column in list(df.columns) if column in args['aggregation_fields'].split(',')]

    columns_to_keep = list(set(aggregation_fields + list(aggregation_types.keys()) + list(aggregation_names.keys()) + boolean_occurrences))

    for boolean in boolean_occurrences:
      df[boolean] = np.where(df[boolean] == True, 1, 0)
    
    agg_time_field = args['aggregation_time_field']
    if agg_time_field is not None and agg_time_field in df.columns:
      agg_timespan = args['aggregation_time_span']
      timezone = args['timezone'] if args['timezone'] is not None else tz.tzlocal()
      columns_to_keep = list(set(columns_to_keep + [agg_time_field]))
      df[agg_time_field] = pd.to_datetime(df[agg_time_field])
      df[agg_time_field] = df[agg_time_field].dt.tz_convert(timezone)
      df = df.set_index(agg_time_field)
      aggregation_fields.insert(0, pd.Grouper(level=agg_time_field, freq=f"{agg_timespan}d"))
      
    for column in df.columns:
      if column not in columns_to_keep: df.drop(columns=[column], inplace=True)
    
    aggregated_df = df.groupby(aggregation_fields).agg(aggregation_types)
    aggregated_df.rename(columns=aggregation_names, inplace=True)
    aggregated_df = aggregated_df.reset_index()

    if agg_time_field is not None and agg_time_field in aggregated_df.columns:
      aggregated_df[agg_time_field] = aggregated_df[agg_time_field].dt.date

    return aggregated_df
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when trying to aggregate the raw documents. Here's the exception: \n\n{e}"))