import os
import re

dot = '.'
pipe = '|'
slash = '/'


def check_version(v):
    """
    :param v: version input
    :return: Boolean.
    """
    try:
        return True
    except ValueError as msg:
        return False


def check_path(path):
    """
    Checks whether the input path is a valid path to a directory or file or not
    Reinforced with regex and os.path
    :param path: String
    :return: Boolean.
    """
    isFile, isDir = False
    check_file = re.compile("^(\/+.{0,}){0,}\.\w{1,}$")
    check_directory = re.compile("^(\/+.{0,}){0,}$")
    print(check_file.match(path), check_directory.match(path))
    if check_file.match(path):
        result = os.path.isfile(path)
        isFile = True
    elif check_directory.match(path):
        result = os.path.isdir(path)
        isDir = True
    else:
        result = False
    return result


def extract_ids(directory, file_name, version, data_node, isFile, isDir):
    """
    This method is used to automatically extract the different ids based on the path, version and filenames.

    :param directory : String (path to the dataset files, this must be a directory not a file)
    :param file_name : list of String (filename list existing under the path)
    :param version : String (version input)
    :return: 3-tuple of String (automatically generated dataset_id, master_id and DRS_id)
    """
    # Note that the path if this method is solicited is already tested and is a valid path.
    # If this method is to be used elsewhere, make sure to test the path via regex or os lib.

    # DRS_id seems to be the base id.
    # All the other ids can be built from it.
    # Hence I used it as a buffer.
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


def extract_file_name(path):
    """
    This function is used to extract the file_name from the path if the path is a file path.
    The directory or file test is performed elsewhere.
    :param path: string
    :return: directory, filename (string couple)
    """
    # The directory is generated from the path - filename.
    filename = os.path.basename(path)
    return filename, path.replace(filename, '')
