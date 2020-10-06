from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
import requests, sys, json
from requests.auth import HTTPBasicAuth
from .utility_functions import *
from tqdm import *
from .csv_handlers import *

def build_es_connection(args):
  return Elasticsearch( hosts=[{'host': args['host'], 'port': args['port']}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(args['user'], args['password']),
                          use_ssl=args['ssl'],
                          verify_certs=args['cert_verification'],
                          retry_on_timeout=True,
                          timeout=50, ssl_show_warn=False )


def request_to_es(url, query, log, user='', pwd='', timeout=10):
  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  try:
    r = requests.get(url, data=query, headers=headers, auth=HTTPBasicAuth(user, pwd), timeout=10).json()
  except Exception as e:
    log.error(f"\n\nSomething when wrong connecting to the ES instance. Check out the raised exception: \n\n{e}")
    os._exit(os.EX_OK)
  return r

def test_es_connection(args):
  try:
    url = "{url_prefix}://{host}:{port}".format(**args)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(args['user'], args['password']), timeout=10)
    if r.status_code != 200: sys.exit(f"Status code when trying to connect to your host at {url} is not 200. Check out the reason here:\n\n{json.dumps(r.json(), indent=2)}")
  except Exception as e:
    sys.exit(f"Something went wrong when testing the connection to your host. Check your host, port and credentials. Here's the exception:\n\n{e}")

def fetch_es_data(args, starting_date, ending_date, process_name='Main'):
  log = args['log']
  process_number = 0 if process_name == 'Main' else process_name
  process_tmp_subset = 1
  csv_partial_filename = f"{args['export_path'][:-4]}_process{process_number}_{str(process_tmp_subset).zfill(5)}.csv"
  log.info(f"Process {process_name}: starts fetching data from {starting_date} to {ending_date}")
  df_header = ['_id'] + args['fields_to_export'] if not '_id' in args['fields_to_export'] else args['fields_to_export']
  meta_for_extraction = ['_id'] + args['metadata_fields'] if not '_id' in args['metadata_fields'] else args['metadata_fields']
  es_count_query = build_es_query(args, starting_date, ending_date, count_query=True)

  total_hits = request_to_es(args['count_url'], es_count_query, log, args['user'], args['password'])['count']
  pbar = tqdm(total=total_hits, position=process_number, leave=False, desc=f"Process {process_name} - Fetching", ncols=150, mininterval=0.05) if not args['disable_progressbar'] else None
  
  fetched_data = []

  es_instance = build_es_connection(args)
  es_index = args['index']
  scroll_timeout = args['scroll_timeout']
  batch_size = args['batch_size']
  es_query = build_es_query(args, starting_date, ending_date, source=args['fields'])
  try:
    es_data = es_instance.search(
      index = es_index,
      _source = args['fields'],
      scroll = scroll_timeout,
      size = batch_size,
      body = es_query
    )
  except Exception as e:
    log.error(f"\nProcess {process_name}: something went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{e}")
    os._exit(os.EX_OK)
  
  # Save parameters for scrolling
  sid = es_data['_scroll_id']
  scroll_size = len(es_data['hits']['hits'])

  # Process current batch of hits before starting to scroll
  for hit in es_data['hits']['hits']:
    fetched_data.append(add_meta_fields(hit, meta_for_extraction, log)) 

  # Scroll and add hits to the fetched_data list
  while scroll_size > 0:
    es_data = es_instance.scroll(scroll_id=sid, scroll=scroll_timeout)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      fetched_data.append(add_meta_fields(hit, meta_for_extraction, log)) 
      if not args['disable_progressbar']: pbar.update(1)
    # Update the scroll ID
    sid = es_data['_scroll_id']
    # Get the number of results that returned in the last scroll
    scroll_size = len(es_data['hits']['hits'])

    # If this process has already fetched 10M events, create the df, write the partial csv and empty the fetched_data list
    if len(fetched_data) >= args['partial_csv_size']:
        write_csv(csv_partial_filename, df_header, exception_message=f"Something went wrong when trying to write the partial csv {csv_partial_filename}.", list_to_convert=fetched_data)
        process_tmp_subset += 1
        csv_partial_filename = f"{args['export_path'][:-4]}_process{process_number}_{str(process_tmp_subset).zfill(5)}.csv"
        fetched_data = []
    
  write_csv(csv_partial_filename, df_header, exception_message=f"Something went wrong when trying to write the partial csv {csv_partial_filename}.", list_to_convert=fetched_data)
  log.info(f"Process {process_name} has fetched and processed {total_hits} docs. They've been split into {process_tmp_subset} partial csv file(s)")
  return True
