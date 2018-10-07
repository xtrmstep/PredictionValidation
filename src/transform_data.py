import pandas as pd
import os
import glob
import shutil

"""
Algorithm:
    Read and process CSV in chunks using pandas.read_csv(). Each iteration performs sequence of transformations
    above the data and stores results to a destination folder. The operations look as follows:
        1) read a chunk
        2) extract datetime parameters and add to the original dataFrame
        3) group by locations
        4) create a new file or append to existing file for locations  
"""


def clear_folder(folder):
    """ remove all files and subdirectories inside the given folder """
    walk_data = os.walk(folder)
    for root, dirs, files in walk_data:
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def process_all_files(source_files_path, destination_folder):
    """ read and process files in chunk and store to the destination folder """
    clear_folder(destination_folder)
    raw_files = glob.glob(source_files_path)
    print("Found",len(raw_files),"files")

    # get script's folder to build an absolute path to a destination folder
    # absolute path is required for pandas.dataFrame to save data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for raw_file in raw_files:
        print("Reading", raw_file)

        reader = pd.read_csv(raw_file, sep=';', error_bad_lines=False, chunksize=10000)
        chunk_count = 0
        for chunk in reader:
            grouped = chunk.groupby(['LocationID'])
            chunk_count = chunk_count + 1
            print("Processing next chunk...", chunk_count)
            for name, group in grouped:
                # I use assumption that customer name is in the name of files
                customer_name = 'PE' if 'PE' in raw_file else 'TRG'
                destination_abs_file_path = os.path.join(script_dir,
                                                         destination_folder,
                                                         '{}_location_{}.csv'.format(customer_name, name))
                if os.path.exists(destination_abs_file_path):
                    group.to_csv(destination_abs_file_path, sep=';', index=False, mode = 'a', header = False)
                else:
                    group.to_csv(destination_abs_file_path, sep=';', index=False)


def main():
    source_files = '../data/raw/*.csv'
    destination_folder = '../data/processed/'
    process_all_files(source_files, destination_folder)


main()