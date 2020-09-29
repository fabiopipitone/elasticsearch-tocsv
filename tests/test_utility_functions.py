import pytest
import os
from ..elasticsearch_tocsv.helpers.utility_functions import *

def test_final_pw():
  assert final_pw({"password":"pwd"}) == "pwd"
  os.environ['spwd'] = 'testing'
  assert final_pw({"password":"pwd", "secret_password":"spwd"}) == 'pwd'
  assert final_pw({"password":None, "secret_password":"spwd"}) == 'testing'
  assert final_pw({"password":None, "secret_password":None}) == ''

def test_build_source_query():
  assert build_source_query(["field1", "field2"]) == '"_source":["field1", "field2"],'
  assert build_source_query(['field1', 'field2']) == '"_source":["field1", "field2"],'
  assert build_source_query(['field1']) == '"_source":["field1"],'

def test_build_es_query():
  args = {"query_string":"*", "time_field":"timestamp", "starting_date":"2020-05-07T00:00:00+02:00", "ending_date":"2020-08-07T00:00:00+02:00", "source":["field1","field2"]}
  assert build_es_query(args, args['starting_date'], args['ending_date'], size=3, source=args['source']) == \
    '{"_source":["field1", "field2"],"size": 3,"sort":[{"timestamp":{"order":"asc"}}],"query":{"bool":{"must":[{"query_string":{"query":"*"}},{"range":{"timestamp":{"gte":"2020-05-07T00:00:00+02:00","lte":"2020-08-07T00:00:00+02:00"}}}]}}}'
  assert build_es_query(args, args['starting_date'], args['ending_date'], size=3) == \
    '{"size": 3,"sort":[{"timestamp":{"order":"asc"}}],"query":{"bool":{"must":[{"query_string":{"query":"*"}},{"range":{"timestamp":{"gte":"2020-05-07T00:00:00+02:00","lte":"2020-08-07T00:00:00+02:00"}}}]}}}'
  assert build_es_query(args, args['starting_date'], args['ending_date']) == \
    '{"sort":[{"timestamp":{"order":"asc"}}],"query":{"bool":{"must":[{"query_string":{"query":"*"}},{"range":{"timestamp":{"gte":"2020-05-07T00:00:00+02:00","lte":"2020-08-07T00:00:00+02:00"}}}]}}}'
  assert build_es_query(args, args['starting_date'], args['ending_date'], count_query=True) == \
    '{"query":{"bool":{"must":[{"query_string":{"query":"*"}},{"range":{"timestamp":{"gte":"2020-05-07T00:00:00+02:00","lte":"2020-08-07T00:00:00+02:00"}}}]}}}'
  assert build_es_query(args, args['starting_date'], args['ending_date'], count_query=True, size=3) == \
    '{"query":{"bool":{"must":[{"query_string":{"query":"*"}},{"range":{"timestamp":{"gte":"2020-05-07T00:00:00+02:00","lte":"2020-08-07T00:00:00+02:00"}}}]}}}'
  assert build_es_query(args, args['starting_date'], args['ending_date'], count_query=True, size=3)
  args['time_field'] = None 
  assert build_es_query(args, args['starting_date'], args['ending_date'], count_query=True, size=3) == \
    '{"query":{"bool":{"must":[{"query_string":{"query":"*"}}]}}}'

def test_add_meta_fields():
  obj = {"_id":"foo_id", "_type":"doc", "_score":1234, "_index":"foo_index", "_source":{"foofield":"foovalue", "barfield":"barvalue"}}
  assert add_meta_fields(obj, ['_id']) == {"foofield":"foovalue", "barfield":"barvalue", "_id":"foo_id"}
  assert add_meta_fields(obj, ['_id', "_type"]) == {"foofield":"foovalue", "barfield":"barvalue", "_id":"foo_id", "_type":"doc"}
