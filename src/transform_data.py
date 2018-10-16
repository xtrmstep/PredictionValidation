import pandas as pd
import os
import glob
import shutil

"""
Algorithm:
    Read and process CSV in chunks using pandas.read_csv(). Each iteration performs sequence of transformations
    above the data and stores results to a destination folder. The operations look as follows:
        1) read a chunk
        2) NOT READY extract datetime parameters and add to the original dataFrame
        3) group by locations
        4) create a new file or append to existing file for locations  
"""


class DataTransformationProcessor:

    def __init__(self, source_files, destination_folder):
        self._transformation_tasks = [
            self._default_task
        ]
        self._source_files = source_files
        self._destination_folder = destination_folder

    def _default_task(self, customer_name, data_chunk_name, customer_data):
        """ task can return several transformations, each of transformed data should have a distinct file name """
        # get script's folder to build an absolute path to a destination folder
        # absolute path is required for pandas.dataFrame to save data
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(script_dir, self._destination_folder, '{}_location_{}.csv'.format(customer_name, data_chunk_name))
        return [(file, customer_data)]

    def _process_all_files(self, source_files_path, destination_folder):
        """ read and process files in chunk and store to the destination folder """
        self._clear_folder(destination_folder)
        raw_files = glob.glob(source_files_path)
        print("Found",len(raw_files),"files")

        for raw_file in raw_files:
            print("Reading", raw_file)
            reader = pd.read_csv(raw_file, sep=';', error_bad_lines=False, chunksize=10000)
            chunk_count = 0
            for chunk in reader:
                # TODO move grouping to transformation method
                grouped = chunk.groupby(['LocationID'])
                chunk_count = chunk_count + 1
                print("Processing next chunk...", chunk_count)
                for name, group in grouped:
                    # I use assumption that customer name is in the name of files
                    customer_name = 'PE' if 'PE' in raw_file else 'TRG'
                    for task in self._transformation_tasks:
                        # each transformation tasks can return several data transformations
                        transformed_data_array = task(self, customer_name, name, group)
                        for file, data in transformed_data_array:
                            self._save_transformation(file, data)

    @staticmethod
    def _clear_folder(folder):
        """ remove all files and subdirectories inside the given folder """
        walk_data = os.walk(folder)
        for root, dirs, files in walk_data:
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

    @staticmethod
    def _save_transformation(destination_file_path, data_frame):
        if os.path.exists(destination_file_path):
            data_frame.to_csv(destination_file_path, sep=';', index=False, mode='a', header=False)
        else:
            data_frame.to_csv(destination_file_path, sep=';', index=False)

    def execute(self):
        self._process_all_files(self._source_files, self._destination_folder)


if __name__ == "__main__":
    transformation = DataTransformationProcessor('../data/raw/*.csv', '../data/processed/')
    transformation.execute()