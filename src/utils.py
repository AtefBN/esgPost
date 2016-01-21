import os
import re
import subprocess
from custom_exceptions import *

dot = '.'
pipe = '|'
slash = '/'
xml_extension = dot+'xml'
DRS = 'drs_id'
version_str = 'v_'
open_delete_tag = '<delete><query>'
id_str = 'id:'
close_delete_tag = '</query></delete>'
PUBLISH_OP = 'ws_publish'
UNPUBLISH_OP = 'ws_unpublish'


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
                # Found a netCDF file, useful if unique.
                persistent_file_name = file_name
        if number_of_files == 0:
            raise NoNetcdfFilesInDirectoryException
        elif number_of_files == 1:
            is_file = True
            # Adding the filename to the path to get a file instead of a directory.
            path = os.path.join(path, persistent_file_name)
    else:
        raise InvalidPathException
    return is_file, valid_path, path


def unpublish_id(path, version, node):
    base_id = ''
    for index, c in enumerate(path):
        # skipping the slashes '/' from start and end of the path string.
        if (path.startswith(c) or path.endswith(c)) and (index == 0 or index == len(path) - 1):
            continue
        # building the base which coincides with the DRS id.
        elif c != slash:
            base_id += c
        # replacing the inner slashes with dots.
        else:
            base_id += dot
    gen_id = open_delete_tag + id_str + base_id + dot + 'v_' + version + pipe + node.data_node + close_delete_tag
    """
    this generate an ID like the following:
    '<delete><query>id:cmip5.test.v1|esgf-dev.dkrz.de</query></delete>'

    """
    return gen_id


def index(output_path, unpub_id, certificate_file, header_form, session):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via pycurl, however for the
    time being we just use a direct curl command.
    :param output_path: String
    :param certificate_file : path to the certificate_file
    :param header
    :param URL of the webservice.

    :return: Success or failure message: String
    """
    if session.operation == PUBLISH_OP:
        os.chdir(output_path)
        file_list = os.listdir(output_path)
        # loop over the records generated
        for record in file_list:
            # generate path to each file
            if output_path.endswith(slash):
                record_path = output_path + record
            else:
                record_path = output_path + slash + record
            curl_query = create_query(certificate_file, record_path, header_form, session.ws_url)

    elif session.operation == UNPUBLISH_OP:
        curl_query = create_query(certificate_file, unpub_id, header_form, session.ws_url)

    proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    print "Curl output: %s, %s" % (out, err)


def create_query(cert, data, header, wsurl):
    curl_query = "curl --key " + cert + "  --cert " + cert + \
                         " --verbose -X POST -d @" + data + " --header " + header + \
                         " " + wsurl
    return curl_query
