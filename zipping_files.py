# SuperFastPython.com
# create a zip file and add files concurrently with threads
import os
import sys
from os import listdir
from os.path import join
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED, ZIP_BZIP2
import gzip
from threading import Lock
from concurrent.futures import ThreadPoolExecutor


import dif_func as dif_func


# load the file as a string then add it to the zip in a thread safe manner
def add_file_to_zip(lock, handle, filepath):
    # load the data as a string
    filename = os.path.split(filepath)[1]
    with open(filepath, 'r') as file_handle:
        data = file_handle.read()
    # add data to zip
    with lock:
        handle.writestr(filename, data)
    # report progress
    print(f'.added {filename}')


def zip_file(filepath, folder, type_compress=ZIP_DEFLATED):
    # load the data as a string
    if type_compress == ZIP_BZIP2:
        file_ext = ".bz2"
    else:
        file_ext = ".zip"
    with open(filepath, 'rb') as file_handle:
        data = file_handle.read()
    # add data to zip
    source_folder, name = os.path.split(filepath)

    with ZipFile(os.path.join(source_folder, folder, name) + file_ext, "w", compression=type_compress) as file_zip:
        file_zip.writestr(name, data)
    # report progress
    print(f'.added {name}')


# create a zip file
def zip_to_one(path, zip_file_name):
    # list all files to add to the zip
    files = [join(path, f) for f in listdir(path) if os.path.isfile(join(path, f))]
    # create lock for adding files to the zip
    lock = Lock()
    # open the zip file
    with ZipFile(join(path, zip_file_name)+ '.zip', 'w', compression=ZIP_DEFLATED) as handle:
        # create the thread pool
        with ThreadPoolExecutor(100) as exe:
            # add all files to the zip archive
            _ = [exe.submit(add_file_to_zip, lock, handle, f) for f in files]


@dif_func.benchmark
def zip_to_many(path, subfolder, type_compress=ZIP_DEFLATED, thread_num=1):
    # list all files to add to the zip
    try:
        os.mkdir(path+'\\'+subfolder)
    except OSError:
        pass
    files = [join(path, f) for f in listdir(path) if os.path.isfile(join(path, f))]
    # for f in files:
    #     zip_file(f, path+'\\'+subfolder, type_compress)
    with ThreadPoolExecutor(thread_num) as exe:
        # add all files to the zip archive
        _ = [exe.submit(zip_file, f, path+'\\'+subfolder, type_compress) for f in files]


def un_gzip_to_memory(zip_path):
    with gzip.open(zip_path, 'rb') as gzip_file:
        extracted_data = gzip_file.read()
    return extracted_data



def com_help():
    print("Using zipping_files:")
    print("zipping_files path command parameter [option1] [option2]")
    print("commands: 'one' or 'many'")
    print("if command is 'one' parameter is zip file name wo extension")
    print("if command is 'many' parameter is subfolder name")
    print("option1 - zip algoritm 1-")


def main():
    if len(sys.argv) in (4, 5, 6):
        path = sys.argv[1]
        command = sys.argv[2]
        parameter = sys.argv[3]
        if command == "one":
            zip_to_one(path, parameter)
        elif command == "many":
            zip_to_many(path, parameter, ZIP_BZIP2, 50)
        else:
            com_help()
    else:
        com_help()


# entry point
if __name__ == '__main__':
    main()
