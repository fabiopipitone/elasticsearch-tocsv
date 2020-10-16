from concurrent.futures import ProcessPoolExecutor
import multiprocessing, urllib3
from elasticsearch_tocsv.helpers.csv_handlers import *
from elasticsearch_tocsv.helpers.utility_functions import *
from elasticsearch_tocsv.helpers.interval_builders import *
from elasticsearch_tocsv.helpers.connection_tools import *
from elasticsearch_tocsv.helpers.arguments_checkers import *
from elasticsearch_tocsv.helpers.color_wrappers import *
from elasticsearch_tocsv.utils.TqdmLoggingHandler import TqdmLoggingHandler
from elasticsearch_tocsv.utils.CustomLogger import CustomLogger

def main():
  log = CustomLogger(__name__).logger
  args = fetch_arguments()

  if not args['cert_verification']:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  args = check_arguments_conflicts(args, log)

  if args['aggregate_only'] == False:
    check_csv_already_written(args['export_path'])

    log.info(wrap_green(bold("################### EXTRACTING DATA ###################\n")))

    if args['enable_multiprocessing']:
      log.info(wrap_blue('Connection to ES host established -- Multiprocessing enabled\n'))
      processes_to_use = min(multiprocessing.cpu_count(), args['process_number']) if args['process_number'] != None else multiprocessing.cpu_count()
      
      # Split total time range in equals time intervals so to let each process work on a partial set of data and store the resulting list as a sublist of the lists_to_join list
      if args['load_balance_interval'] == None:
        log.info(wrap_blue("No --load_balance_interval has been set. Intervals will be created on a time basis\n"))
        processes_intervals = make_time_intervals(args, processes_to_use, args['starting_date'], args['ending_date'])
      else:
        log.info(wrap_blue("Having set the --load_balance_interval option, intervals will be created on a load basis as far as possible according to the -lbi set\n"))
        processes_intervals = make_intervals_by_load(args, processes_to_use, args['starting_date'], args['ending_date'])

      # Check if to create aggregated csv file
      log.info(aggregation_log(args['aggregated_export_path'], args['aggregation_fields'], args['aggregation_types']))

      # Build the list of arguments to pass to the function each process will run
      process_function_arguments = [[args for i in range(processes_to_use)], *processes_intervals, [i for i in range(processes_to_use)]]
      
      processes_done = [None for i in range(processes_to_use)]

      # Create and start processes_to_use number of processes
      with ProcessPoolExecutor(processes_to_use) as executor:
        processes_done = executor.map(fetch_es_data, *process_function_arguments)

      while list(set(processes_done)) != [True]:
        continue
    else:
      log.info(wrap_blue('Connection to ES host established -- Single process run\n'))

      # Check if to create aggregated csv file
      log.info(aggregation_log(args['aggregated_export_path'], args['aggregation_fields'], args['aggregation_types']))
      
      fetch_es_data(args, args['starting_date'], args['ending_date'])

    # Joining partial csv files previously created into a single one
    log.info(wrap_blue('Joining partial csv files\n'))
    final_df = join_partial_csvs(args['export_path'][:-4])
    
    # Remove duplicates
    final_df = remove_duplicates(args, final_df)
    final_df_newcolumns = rename_df_columns(final_df, args['rename_fields'])
    fields_to_export = [args['rename_fields'][field] if field in args['rename_fields'].keys() else field for field in args['fields_to_export']]

    # Write the CSV from the dataframe
    log.info(wrap_blue(f'Creating the csv at the following export path --> {args["export_path"]}.\n'))
    write_csv(args['export_path'], fields_to_export, "Something went wrong when trying to write the final csv after the merge of the partial csv files. The partial csv files won't be deleted.", df=final_df_newcolumns)

    # Delete partial csvs
    if not args['keep_partials']: 
      log.info(wrap_blue('Deleting partial csv files\n'))
      delete_partial_csvs(args['export_path'][:-4])

  # Create aggregation file if needed
  if args['aggregation_fields'] != None:
    if args['aggregate_only']: 
      log.info(wrap_green(bold("################# AGGREGATING RAW FILE ################\n")))
    else:
      args['df_ready'] = final_df
    log.info(wrap_blue('Creating the csv file with aggregated data\n'))
    if not args['aggregation_fields_checked']: check_valid_agg_related_fields(args, log)
    file_to_aggregate = args['raw_input_file'] if args['aggregate_only'] else args['export_path']
    aggregated_df = aggregate_fields(file_to_aggregate, args, log, args['df_ready'])
    aggregated_df = rename_df_columns(aggregated_df, args['rename_fields'])
    write_csv(args['aggregated_export_path'], None, f"Something when wrong when trying to write the aggregated dataframe to {args['aggregated_export_path']}. If you exported the raw data from Elasticsearch, the raw csv file will be available at \"{args['export_path']}\". Please use it with the --aggregate_only flag and a proper --aggregation_types and --aggregation_fields configuration to generate your aggregated file.", df=aggregated_df, index=False)
    log.info(wrap_blue(f"Aggregated file has been created at path \"{args['aggregated_export_path']}\"\n"))

  log.info(wrap_green(bold("################# EXITING SUCCESSFULLY ################\n")))

if __name__ == "__main__":
  main()

