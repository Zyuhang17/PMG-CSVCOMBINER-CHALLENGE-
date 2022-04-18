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
import pandas as pd
import sys
import os
import csv


class csv_combiner:

    ## main function
    def main(self, argv: list):

        ## preset an empty list that appends the dataframe 
        combined_csv = []

        ## check if the command line can be excuted correctly
        if len(argv) <= 1:
            print("Error: Invalid input. Check your input file")
            return
            
        else:
            # start reading from the first csv file after the .py file
            for file_path in argv[1:]:
                # read as chunks to prevent memory issues
                # escape character to get rid of the "\" character when reading
                for chunk in pd.read_csv(file_path, chunksize=10000, escapechar='\\'):

                    # get the file name from the path and add to the dataframe
                    chunk['filename'] = os.path.basename(file_path)

                    # append to the empty data list
                    combined_csv.append(chunk)

            # flag to indicate if a header should be added
            header = True

            # Print to csv 
            for chunk in combined_csv:
                # quoting get rids of the quotation marks
                print(chunk.to_csv(index=False, header=header, line_terminator='\n', chunksize=10000, quoting = csv.QUOTE_NONE), end='')
                header = False


## execution:
combiner = csv_combiner()
combiner.main(sys.argv)
