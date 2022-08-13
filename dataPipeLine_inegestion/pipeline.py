import utils as utils # utils.py is in the same directory as my other files. 
import timeit
import dask.dataframe as dd
import os

##### Create timer functions to measure difference in reading files b/w pandas and dask
import_module = "import dask.dataframe as dd"
timer = '''
def dask_time(source_file, in_del):
    return dd.read_csv(source_file, in_del)
'''
pandas_import = "import pandas as pd"
pandas_timer = '''
def pand_timer(source_file, in_del):
    return pd.read_csv(source_file, in_del)
'''

if __name__ == "__main__":
    #yaml file allows my pipeline to be more dynamic and I can adjust requirements in yaml file if any issues pop up
    # Functions are found in utils.py 
    config_data = utils.read_config_file("/home/bioinfo/DataGlacier/DataGlacier_projects/dataPipeLine_inegestion/config.yaml") 
    source_file = utils.read_config(config_data)

    # Testing out times b/w pandas and dask 
    in_del = config_data['inbound_delimiter']
    dask_time = timeit.timeit(stmt=timer, setup=import_module)
    pandas_time = timeit.timeit(stmt=pandas_timer, setup=pandas_import)
    print(f'Time to load Dask df: {dask_time:.4f}.') # Dask is slightly faster, and has more scalability
    print(f'Time to load Pandas df: {pandas_time:.4f}.')

    # Reading in data, validating column headers, and performing downstream process
    ddf = dd.read_csv(source_file, dtype={'average_abund': 'object',
       'f_match': 'object',
       'f_match_orig': 'object',
       'f_orig_query': 'object',
       'f_unique_to_query': 'object',
       'f_unique_weighted': 'object',
       'gather_result_rank': 'object',
       'intersect_bp': 'object',
       'median_abund': 'object',
       'query_bp': 'object',
       'query_filename': 'object',
       'remaining_bp': 'object',
       'std_abund': 'object',
       'unique_intersect_bp': 'object'})

    print((config_data['columns']))
    print(len(config_data['columns']))

    if utils.col_header_val(ddf,config_data)==0:
        print("validation failed")
        # write code to reject the file
    else:
        print("Validation Passed")
        ddf.to_csv('large_file.csv.gz' % ddf,
                    sep=config_data['outbound_delimiter'],
                    header=True,
                    index=False,
                    compression='gzip',
                    line_terminator='\n')
        print(f"File size: {os.path.getsize('large_file.csv'):.1f}")
        print("Number of columns: ",len(ddf.columns))
        print("Number of rows: ", len(ddf.index))
    