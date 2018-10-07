import os
import glob
import shutil


def clear_folder(folder):
    """ remove all files and subdirectories inside the given folder """
    walk_data = os.walk(folder)
    for root, dirs, files in walk_data:
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

