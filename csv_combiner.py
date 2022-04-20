"""
Title: PMG Coding Challenge
Author: Yuhang Zhang
file: csv_combiner.py
Description: A command line program that takes several CSV files as arguments and output a new 
    combined csv file.
Input: CSV files that have the same columns. 
Date: 17/04/2022
"""
## import packages
from typing import Sized
import pandas as pd
import sys
import os
import csv
# import time
# start_time = time.time()
# print("--- %s seconds ---" % (time.time() - start_time))

class csv_combiner_small:

    ## main function
    def main(self, argv: list):

        ## preset an empty list that appends the dataframe 
        combined_csv = pd.DataFrame()
        df = pd.DataFrame()

        ## check if the command line can be excuted correctly
        if len(argv) <= 1:
            print("Error: Invalid input. Check your input file")
            return
            
        else:
            # start reading from the first csv file after the .py file
            for file_path in argv[1:]:
                # escape character to get rid of the "\" character when reading
                df = pd.read_csv(file_path, escapechar='\\') # alternatively, dask.dataframe.read_csv can be useful as well since it does not load into memeory but ready to use
                df['filename'] = os.path.basename(file_path)
                combined_csv = pd.concat([combined_csv, df])

            print(combined_csv.to_csv(index=False, header=True, line_terminator='\n', quoting = csv.QUOTE_NONE))


class csv_combiner_large:

    ## main function
    def main(self, argv: list):

        ## preset an empty list that appends the dataframe 
        combined_csv = []
        df = []

        ## check if the command line can be excuted correctly
        if len(argv) <= 1:
            print("Error: Invalid input. Check your input file")
            return
            
        else:
            # start reading from the first csv file after the .py file
            for file_path in argv[1:]:
                # read as smaller dataframes to prevent memory issues
                # escape character to get rid of the "\" character when reading
                for chunk in pd.read_csv(file_path, chunksize=100000, escapechar='\\'):

                    # get the file name from the path and add to the dataframe
                    chunk['filename'] = os.path.basename(file_path)
                    # append to the empty data list
                    combined_csv.append(chunk)
              
                    # We can test the memory use for a single line of data: we are reading approximately 20 mb per chunk
                    #print(chunk.memory_usage(index=False, deep=True) / chunk.shape[0])

            header = True
            # Print to csv 
            for chunk in combined_csv:
                # quoting get rids of the quotation marks
                # header set to false after the first loop since we do not need more headers to print out
                # end = '' get rids of the blank line in between the two files
                print(chunk.to_csv(index=False, header=header, line_terminator='\n', chunksize=100000, quoting = csv.QUOTE_NONE), end='')
                header = False
                

    

## execution:

## use either function by total file size.
total_size = 0
for file_path in sys.argv[1:]:
    size = os.path.getsize(file_path)
    total_size = size + total_size

if total_size <= 1.25 * 10**8: # Set 1 GB as threshold
    combined_csv = csv_combiner_small()
else:
    combined_csv = csv_combiner_large()

combined_csv.main(sys.argv)

