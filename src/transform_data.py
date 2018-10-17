import pandas as pd
import numpy as np
import os
import glob
import shutil
import datetime_helper as dth

"""
Algorithm:
    This is simplified custom pipeline to process big data files using Pandas. It reads and processes CSV in chunks
    using pandas.read_csv(). After that it applies a pipeline to each of the chunks. Pipeline denotes a sequence of tasks
    to perform upon data. 
    In a simplified form the workflow looks as follows:
        1) read chunks
        2) apply pipelines to each chunk separately
        3) after each pipeline store result to disk (create or update files)
    Pipeline workflow looks as follows:
        1) the first task should designate all data block to smaller groups, which will be stored to disk
            if not required then it should return only one group
            it returns a tuple: file name and data
        2) other tasks perform operations upon groups, but they don't break the original designation
        3) it returns transformed data as an array of tuples (file, data)  
"""


class DataTransformationProcessor:

    def __init__(self, source_files, destination_folder):
        self._transformation_pipelines = [
            self._pipeline_extract_locations,
            self._pipeline_extract_cities
        ]
        self._source_files = source_files
        self._destination_folder = destination_folder

    def _pipeline_extract_locations(self, customer_name, customer_data):
        """ task can return several transformations, each of transformed data should have a distinct file name """

        data_blocks = self._default_pipeline_task_designate_chunks(customer_name, customer_data)
        # other tasks go here...
        data_blocks = self._add_datetime_parts(data_blocks)
        return data_blocks

    def _pipeline_extract_cities(self, customer_name, customer_data):
        data_blocks = self._task_designate_cities(customer_name, customer_data)
        # other tasks go here...
        return data_blocks

    def _task_designate_cities(self, customer_name, customer_data):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(script_dir, self._destination_folder, '{}_cities.csv'.format(customer_name))

        cities = customer_data['City1'].unique()
        cities = np.asarray(list(filter(bool, cities)))
        cities = pd.DataFrame({'City': cities})
        return [(file, cities)]

    def _default_pipeline_task_designate_chunks(self, customer_name, customer_data):
        """ designate data to chunks and files to store them on disk """
        # get script's folder to build an absolute path to a destination folder
        # absolute path is required for pandas.dataFrame to save data
        script_dir = os.path.dirname(os.path.abspath(__file__))
        grouped = customer_data.groupby(['LocationID'])
        result = []
        for name, group in grouped:
            file = os.path.join(script_dir, self._destination_folder, '{}_location_{}.csv'.format(customer_name, name))
            result += [(file, group)]
        return result

    @staticmethod
    def _add_datetime_parts(data_blocks):
        for file, data in data_blocks:
            data[['dt_year', 'dt_month', 'dt_day', 'dt_day_of_year', 'dt_day_of_week', 'dt_hour']] =\
                data.apply(lambda df: dth.to_date_parts(df['SalesDate']), axis=1)

        return data_blocks

    def _process_all_files(self, source_files_path, destination_folder):
        """ read and process files in chunk and store to the destination folder """
        self._clear_folder(destination_folder)
        raw_files = glob.glob(source_files_path)
        print("Found",len(raw_files),"files")

        for raw_file in raw_files:
            print("Reading", raw_file)
            reader = pd.read_csv(raw_file, sep=';', error_bad_lines=False, chunksize=10000)
            # I use assumption that customer name is in the name of files
            customer_name = 'PE' if 'PE' in raw_file else 'TRG'
            chunk_count = 0
            for chunk in reader:
                chunk_count = chunk_count + 1
                print("Processing next chunk...", chunk_count)
                for pipeline in self._transformation_pipelines:
                    # each transformation tasks can return several data transformations
                    transformed_data_array = pipeline(customer_name, chunk)
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
