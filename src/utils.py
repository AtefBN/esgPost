__author__ = 'root'
import os
import re


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
    check_file = re.compile("^(\/+\w{0,}){0,}\.\w{1,}$")
    check_directory = re.compile("^(\/+\w{0,}){0,}$")
    result = False
    if check_file.match(path):
        result = os.path.isfile(path)
    elif check_directory.match(path):
        result = os.path.isdir(path)
    else:
        result = False
    return result
