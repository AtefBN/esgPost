import getopt
import sys
import ConfigParser

from scan_and_extract import extract_from_file
from ws_curl import index
from utils import *

usage = """
schema
file_type : Dataset or file
version : Select the version of the file to be published
unpublish/publish : Select either publish or unpublish. But exclusively one at the time. Otherwise the algorithm will select the
latest input.
mapfile : precise the location of the already generated mapfile of the dataset you wish to unpublish.
"""

# Retrieving key values from ini file.
config = ConfigParser.ConfigParser()
config.read('test.ini')
cert_file = config.get('generic', 'certificate_file')
header = config.get('generic', 'header')
temp_dir = config.get('generic', 'temp_dir')
index_node = config.get('generic', 'index_node')
data_node = config.get('generic', 'data_node')
ws_post_url = config.get('generic', 'ws_publish')
PUBLISH_OP = "ws_publish"
UNPUBLISH_OP = "ws_unpublish"
version = 1


def main():
    argv = sys.argv[1:]
    mandatory_options_dict = {}
    path = 'home/esg-user/'
    operation = PUBLISH_OP
    mapfile = None
    result_path = None
    try:
        args, last_args = getopt.getopt(argv, "", ["help", "schema=", "file_type=", "path=", "version=", "publish",
                                                   "unpublish", "mapfile="])
    except getopt.error:
        print sys.exc_value
        print usage
        sys.exit(0)
    for o, a in args:
        if o in ("-h", "--help"):
            print(usage)
            sys.exit()
        elif o in ("-s", "--schema"):
            mandatory_options_dict['schema'] = a
        elif o == "--file_type":
            if check_version(a):
                mandatory_options_dict['type'] = a
        elif o == "--version":
            mandatory_options_dict['version'] = a
            vers = a
        elif o == "--path":
            test_path, isFile, isDir = check_path(a)
            if test_path:
                mandatory_options_dict['path'] = a
        elif o == "--unpublish":
            operation = UNPUBLISH_OP
        elif o == "--publish":
            operation = PUBLISH_OP
        # TODO change the mapfile variable name.
        elif o == "--mapfile":
            test_path, isFile, isDir = check_path(a)
            if test_path:
                mapfile = a
        else:
            assert False, "unhandled option"
    # Result path variable contains the path of the generated
    # XML descriptor that will be pushed to solr for indexation
    if operation == PUBLISH_OP:
        result_path, doc = extract_from_file(mandatory_options_dict, path, temp_dir, index_node, data_node, vers,
                                             isFile, isDir)
    elif operation == UNPUBLISH_OP:
        result_path = mapfile
    else:
        print("Please specify the operation.")
    # This url depends on the type of operation chosen via the options.
    URL = config.get('generic', operation)
    index(result_path, doc, cert_file, header, URL)


if __name__ == "__main__":
    main()
