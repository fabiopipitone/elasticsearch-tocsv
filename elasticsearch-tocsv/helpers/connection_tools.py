from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
import requests, logging, sys
from requests.auth import HTTPBasicAuth

def build_es_connection(args):
  return Elasticsearch( hosts=[{'host': args['host'], 'port': args['port']}],
                          connection_class=RequestsHttpConnection,
                          http_auth=(args['user'], args['password']),
                          use_ssl=args['ssl'],
                          verify_certs=args['cert_verification'],
                          retry_on_timeout=True,
                          timeout=50, ssl_show_warn=False )


def request_to_es(url, query, user='', pwd='', timeout=10):
  headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
  try:
    r = requests.get(url, data=query, headers=headers, auth=HTTPBasicAuth(user, pwd), timeout=10).json()
  except Exception as e:
    logging.error("\n\nSomething when wrong connecting to the ES instance. Check out the raised exception: \n\n{}".format(e))
    os._exit(os.EX_OK)
  return r

def test_es_connection(args):
  try:
    url = "{}://{}:{}".format(args['url_prefix'], args['host'], args['port'])
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(args['user'], args['password']), timeout=10)
    if r.status_code != 200: sys.exit("Status code when trying to connect to your host at {} is not 200. Check out the reason here:\n\n{}".format(url, json.dumps(r.json(), indent=2)))
  except Exception as e:
    sys.exit("Something went wrong when testing the connection to your host. Check your host, port and credentials. Here's the exception:\n\n{}".format(e))
