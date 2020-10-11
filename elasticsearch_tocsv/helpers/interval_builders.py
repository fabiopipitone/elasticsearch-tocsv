import dateutil.parser as dateparser
from datetime import datetime, timedelta
from .utility_functions import *
from .connection_tools import *
from .color_wrappers import *
from tqdm import *
from .arguments_checkers import get_actual_bound_dates

def make_intervals_by_load(args, processes, starting_date, ending_date):
  log = args['log']
  starting_date, ending_date = get_actual_bound_dates(args, starting_date, ending_date)
  total_count_query = build_es_query(args, starting_date, ending_date, count_query=True)
  total_hits = request_to_es(args['count_url'], total_count_query, log, args['user'], args['password'], verification=args['certificate_path'])['count']
  sdate_in_seconds = dateparser.parse(starting_date).timestamp()
  edate_in_seconds = dateparser.parse(ending_date).timestamp()
  dates_for_processes = [[], []]
  sdate = starting_date
  multiplier = 1
  edate = datetime.fromtimestamp(sdate_in_seconds + multiplier * args['load_balance_interval']).astimezone(args['timezone']).isoformat()
  pbar = tqdm(total=total_hits, position=1, leave=True, desc="Building load_weighted time intervals", ncols=150, mininterval=0.05) if not args['disable_progressbar'] else None
  if args['disable_progressbar']: log.info(wrap_blue("Building load_weighted time intervals. This operation might take a while depending on the ration total_hits_counted/load_balance_interval..."))
  while len(dates_for_processes[0]) < processes:
    partial_count_query = build_es_query(args, sdate, edate, count_query=True) 
    hits = request_to_es(args['count_url'], partial_count_query, log, args['user'], args['password'], verification=args['certificate_path'])['count']
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