import os
import re
from exceptions import *
dot = '.'
pipe = '|'
slash = '/'


def check_version(v):
    """
    :param v: vers input
    :return: Boolean.
    """
    try:
        return True
    except ValueError as msg:
        raise InvalidVersionNumber


def check_path(path):
    """
    Checks whether the input path is a valid path to a directory or file or not
    Reinforced with regex and os.path
    The couple booleans (is_file, valid_path) help determine 3 states : the path is a file, the path is a directory
    or the path is not valid. (True, True)=file, (False, True)=directory, (False, False)=Not a valid path.
    :param path: String
    :return: Boolean couple.
    """
    # This variable supposes the path is not a file.
    # It could be either a directory or not a valid path at all.
    is_file = False
    check_file = re.compile("^(\/+.{0,}){0,}\.\w{1,}$")
    check_directory = re.compile("^(\/+.{0,}){0,}$")
    print(check_file.match(path), check_directory.match(path))
    if check_file.match(path):
        valid_path = os.path.isfile(path)
        is_file = True
    elif check_directory.match(path):
        valid_path = os.path.isdir(path)
        os.chdir(path)
        file_list = os.listdir(path)
        number_of_files = 0
        for file_name in file_list:
            if '.nc' in file_name:
                number_of_files += 1
        if number_of_files == 0:
            raise NoNetcdfFilesInDirectoryException
        elif number_of_files == 1:
            is_file = True
            # Adding the filename to the path to get a file instead of a directory.
            if path.endswith(slash):
                path += file_name
            else:
                path += slash + file_name
    else:
        raise InvalidPathException
    return is_file, valid_path


def extract_ids(directory, file_name, version, data_node, is_file):
    """
    This method is used to automatically extract the different ids based on the path, vers and filenames.

    :param directory : String (path to the dataset files, this must be a directory not a file)
    :param file_name : list of String (filename list existing under the path)
    :param version : String (vers input)
    :return: 3-tuple of String (automatically generated dataset_id, master_id and DRS_id)
    """
    # Note that the path if this method is solicited is already tested and is a valid path.
    # If this method is to be used elsewhere, make sure to test the path via regex or os lib.

    # DRS_id seems to be the base id.
    # All the other ids can be built from it.
    # Hence I used it as a buffer and base_id.
    drs_id = ''

    for index, c in enumerate(directory):
        # skipping the slashes '/' from start and end of the path string.
        if (directory.startswith(c) or directory.endswith(c)) and (index == 0 or index == len(directory) - 1):
            print(c)
            continue
        # building the base which coincides with the DRS id.
        elif c != slash:
            drs_id += c
        # replacing the inner slashes with dots.
        else:
            drs_id += dot
    # Building the master and dataset id out of the base id.
    master_id = drs_id + dot + file_name
    dataset_id = drs_id + dot + version + pipe + data_node
    id = drs_id + file_name + version
    instance_id = drs_id + version
    return drs_id, dataset_id, master_id, id, instance_id



