import os, sys, glob
import pandas as pd
from .color_wrappers import *

def check_csv_already_written(filename, silent_mode, log, operation_type):
  if os.path.exists(filename):
    overwrite_file = 'y' if silent_mode else ''
    log.warning(wrap_orange(f"There is already a csv file at the given path ({filename}) that will be overwritten."))
    while overwrite_file not in ['y', 'n']:
      overwrite_file = input(wrap_orange(f"\nDo you want to overwrite it? (y/n)")).lower().strip()
    if overwrite_file == 'n':
      sys.exit(wrap_red(wrap_blue(f"\nExiting script not to overwrite the file. No {operation_type} has been run.")))
    elif overwrite_file != 'y':
      print("Sorry, I can't understand the answer. Please answer with 'y' or 'n'")
  return filename

def write_csv(export_path, fields_to_export, separator, decimal_separator, decimal_rounding, exception_message='', df=None, list_to_convert=False, index=False, new_fields={}):
  df = pd.DataFrame(list_to_convert, columns=fields_to_export) if list_to_convert else df
  if df is not None: 
    df = df.round(decimal_rounding)
    try:
      if fields_to_export is None:
        df.to_csv(export_path, index=index, sep=separator, decimal=decimal_separator)
      else:
        df.to_csv(export_path, index=index, columns=fields_to_export, sep=separator, decimal=decimal_separator)
    except Exception as e:
      sys.exit(wrap_red(f"{exception_message} Here's the exception: \n\n{e}"))

def join_partial_csvs(filename, separator, decimal_separator):
  try: 
    list_of_csvs = sorted([csv for csv in glob.glob(f"{filename}_process*.csv")])
    final_df = pd.concat([pd.read_csv(csv, sep=separator, decimal=decimal_separator) for csv in list_of_csvs])
    return final_df
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when trying to merge partial csv files previously created. The partial csv files won't be deleted. Here's the exception: \n\n{e}"))

def delete_partial_csvs(basic_filename):
  try:
    list_of_csvs = sorted([csv for csv in glob.glob(f"{basic_filename}_process*.csv")])
    for csv in list_of_csvs:
      os.remove(csv)
    return True
  except Exception as e:
    sys.exit(wrap_red(f"Something went wrong when trying to delete partial csv files previously created. Here's the exception: \n\n{e}"))