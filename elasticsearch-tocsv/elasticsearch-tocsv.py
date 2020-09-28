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
from helpers.csv_handlers import *
from helpers.utility_functions import *
from helpers.interval_builders import *
from helpers.connection_tools import *
from helpers.arguments_checkers import *
from utils.TqdmLoggingHandler import TqdmLoggingHandler
from utils.CustomLogger import CustomLogger

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

def main():
  args = fetch_arguments()
  args = check_arguments_conflicts(args)
  test_es_connection(args)
  check_csv_already_written(args['export_path'])

  log.info("################ EXTRACTING DATA ################\n")

  if args['enable_multiprocessing']:
    log.info('Connection to ES host established -- Multiprocessing enabled\n')
    processes_to_use = min(multiprocessing.cpu_count(), safe_toint_cast(args['process_number'])) if args['process_number'] != None else multiprocessing.cpu_count()
    
    # Split total time range in equals time intervals so to let each process work on a partial set of data and store the resulting list as a sublist of the lists_to_join list
    if args['load_balance_interval'] == None:
      log.info("No --load_balance_interval has been set. Intervals will be created on a time basis\n")
      processes_intervals = make_time_intervals(args, processes_to_use, args['starting_date'], args['ending_date'])
    else:
      log.info("Having set the --load_balance_interval option, intervals will be created on a load basis as far as possible according to the -lbi set\n")
      processes_intervals = make_intervals_by_load(args, processes_to_use, args['starting_date'], args['ending_date'], log)

    # Build the list of arguments to pass to the function each process will run
    process_function_arguments = [[args for i in range(processes_to_use)], *processes_intervals, [i for i in range(processes_to_use)]]
    
    processes_done = [None for i in range(processes_to_use)]

    # Create and start processes_to_use number of processes
    with ProcessPoolExecutor(processes_to_use) as executor:
      processes_done = executor.map(fetch_es_data, *process_function_arguments)
  else:
    log.info('Connection to ES host established -- Single process run\n')
    fetch_es_data(args, args['starting_date'], args['ending_date'])

  while list(set(processes_done)) != [True]:
    continue

  # Joining partial csv files previously created into a single one
  log.info('Joining partial csv files\n')
  final_df = join_partial_csvs(args['export_path'][:-4])
  
  # Remove duplicates
  final_df = remove_duplicates(args, final_df, log)

  # Write the CSV from the dataframe
  log.info('Creating the csv at the following export path --> "{}".\n'.format(args['export_path']))
  write_csv(args['export_path'], args['fields_to_export'], "Something went wrong when trying to write the final csv after the merge of the partial csv files. The partial csv files won't be deleted.", df=final_df)

  # Delete partial csvs

  if not args['keep_partials']: 
    log.info('Deleting partial csv files\n')
    delete_partial_csvs(args['export_path'][:-4])

  log.info("################ EXITING SUCCESSFULLY ################\n")

if __name__ == "__main__":
  log = CustomLogger(__name__).logger
  main()

