from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from requests.exceptions import SSLError as RequestsSSLError
import requests, sys, json
from ssl import create_default_context
from requests.auth import HTTPBasicAuth
from .utility_functions import *
from tqdm import *
from .csv_handlers import *
from .color_wrappers import *

def build_es_connection(args):
  return Elasticsearch( hosts=[{'host': args['host'], 'port': args['port']}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(args['user'], args['password']),
                          scheme=args['url_prefix'],
                          verify_certs=args['verify'],
                          retry_on_timeout=True,
                          timeout=50, ssl_show_warn=False )


def request_to_es(url, query, log, user='', pwd='', timeout=10, verification=False):
  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  try:
    r = requests.get(url, data=query, headers=headers, auth=HTTPBasicAuth(user, pwd), timeout=10, verify=verification).json()
  except Exception as e:
    log.error(wrap_red(f"\n\nSomething when wrong connecting to the ES instance. Check out the raised exception: \n\n{e}"))
    os._exit(os.EX_OK)
  return r

def test_es_connection(args, log):
  try:
    url = "{url_prefix}://{host}:{port}".format(**args)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    check_valid_certificate_combinations(args, log)
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(args['user'], args['password']), timeout=10, verify=args['verify'])
    if r.status_code != 200: sys.exit(wrap_red(f"Status code when trying to connect to your host at {url} is not 200. Check out the reason here:\n\n{json.dumps(r.json(), indent=2)}"))
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when testing the connection to your host. Check your host, port, credentials and certificates (if not ignored). Check your ES instance is still running, too. Here's the exception:\n\n{e}"))

def check_valid_certificate_combinations(args, log):
  if args['cert_verification'] is True and args['certificate_path'] == '':
    log.warning(wrap_orange(f"You set --cert_verification to True but no --certificate_path to fetch a local certificate. If the certificate of the ES instance you are trying to connect to doesn't have a root CA in its CA chain (e.g. self-signed certificate) the validation is going to fail.\n"))
    test_certificate(args, "You didn't set a path to a certificate yet you required a certificate verification. Are you sure your ES instance has a certificate signed by a valid root CA? ")
  elif args['cert_verification'] is True:
    test_certificate(args, f"There's a mismatch between the certificate you passed ({args['certificate_path']}) and the one of the ES instance. ")
  elif args['ssl'] is True and args['cert_verification'] is False:
    print(wrap_orange("\nYou're connecting to a ES instance over SSL but without any certificate verification. Be sure you know the risks (e.g. MITM attack)."))
    proceed_without_cert_verification = ''
    while proceed_without_cert_verification not in ['y', 'n']:
      proceed_without_cert_verification = input(wrap_orange(f"\nDo you want to proceed? (y/n)")).lower().strip()
      if proceed_without_cert_verification == 'n':
        sys.exit(wrap_red(wrap_blue("\nExiting script. No connection to the host has been established.")))
      elif proceed_without_cert_verification != 'y':
        print("\nSorry, I can't understand the answer. Please answer with 'y' or 'n'")

def test_certificate(args, exception=''):
  try: 
    url = "{url_prefix}://{host}:{port}".format(**args)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(args['user'], args['password']), timeout=10, verify=args['verify'])
  except RequestsSSLError as e:
    sys.exit(wrap_red(f"Something's wrong with the certificate validation. {exception}\nIf you don't have a valid certificate to pass to --certificate_path, you can work around the problem ignoring the certificate verification (-c False) but check out MITM attack risks first. Here's the exception:\n\n{e}"))

def fetch_es_data(args, starting_date, ending_date, process_name='Main'):
  log = args['log']
  process_number = 0 if process_name == 'Main' else process_name
  process_tmp_subset = 1
  csv_partial_filename = f"{args['export_path'][:-4]}_process{process_number}_{str(process_tmp_subset).zfill(5)}.csv"
  log.info(f"Process {process_name}: starts fetching data from {starting_date} to {ending_date}")
  df_header = ['_id'] + args['fields_to_export'] if not '_id' in args['fields_to_export'] else args['fields_to_export']
  meta_for_extraction = ['_id'] + args['metadata_fields'] if not '_id' in args['metadata_fields'] else args['metadata_fields']
  es_count_query = build_es_query(args, starting_date, ending_date, count_query=True)

  total_hits = request_to_es(args['count_url'], es_count_query, log, args['user'], args['password'], verification=args['certificate_path'])['count']
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
    log.error(wrap_red(f"Process {process_name}: something went wrong when fetching the data from Elasticsearch. Please check your connection parameters. Here's the raised exception: \n\n{e}"))
    os._exit(os.EX_OK)
  
  # Save parameters for scrolling
  sid = es_data['_scroll_id']
  scroll_size = len(es_data['hits']['hits'])

  # Process current batch of hits before starting to scroll
  for hit in es_data['hits']['hits']:
    fetched_data.append(denest(args['fields'], add_meta_fields(hit, meta_for_extraction, log))) 

  # Scroll and add hits to the fetched_data list
  while scroll_size > 0:
    es_data = es_instance.scroll(scroll_id=sid, scroll=scroll_timeout)
    # Process current batch of hits
    for hit in es_data['hits']['hits']:
      fetched_data.append(denest(args['fields'], add_meta_fields(hit, meta_for_extraction, log))) 
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
  log.info(bold(f"Process {process_name} has fetched and processed {total_hits} docs. They've been split into {process_tmp_subset} partial csv file(s)"))
  return True
