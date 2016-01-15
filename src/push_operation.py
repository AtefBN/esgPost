import getopt
import sys
import ConfigParser

from scan_and_extract import extract_metadata
from ws_curl import index
from utils import *
from models import *

usage = """
schema : The schema under which the files are to published/unpublished
path : The path to the netCDF file/files (directory containing many files) you wish to publish/unpublishe
file_type : Dataset or file
vers : The vers of the file to be published
unpublish/publish : Select either publish or unpublish. But exclusively one at the time. Otherwise the algorithm will
select the latest input.
"""

# Retrieving key values from ini file.
config = ConfigParser.ConfigParser()
config.read('/root/PycharmProjects/esgPost/src/test.ini')
cert_file = config.get('generic', 'certificate_file')
header = config.get('generic', 'header')
output_dir = config.get('generic', 'output_dir')
index_node = config.get('generic', 'index_node')
data_node = config.get('generic', 'data_node')
ws_post_url = config.get('generic', 'ws_publish')
PUBLISH_OP = 'ws_publish'
UNPUBLISH_OP = 'ws_unpublish'
# Create the datanode options with the appropriate options
node_instance = Node(data_node, index_node, cert_file, header)


def main():
    argv = sys.argv[1:]
    fields_dictionary = {}
    path = ''
    operation = PUBLISH_OP
    output_path = None
    # Start harvesting user input.
    # TODO add test on compulsory options combinations.(version, path file_type are mandatory)
    try:
        args, last_args = getopt.getopt(argv, "", ["help", "schema=", "path=", "vers=", "publish", "unpublish"])
    except getopt.error:
        print sys.exc_value
        print usage
        sys.exit(0)
    for o, a in args:
        if o in ("-h", "--help"):
            print(usage)
            sys.exit()
        elif o in ("-s", "--schema"):
            schema = a
        elif o == "--vers":
            vers = a
        elif o == "--path":
            is_file, valid_path = check_path(a)
            if valid_path:
                path = a
        elif o == "--unpublish":
            operation = UNPUBLISH_OP
        elif o == "--publish":
            operation = PUBLISH_OP
        else:
            assert False, "unhandled option"
    # Based on input create the Dataset that will host the netCDF files as well as the node_instance.
    # The empty lists are fillers for coming attributes of the dataset,
    # namely attributes coming from files.
    session = Session(operation)
    dataset_instance = Dataset(path, schema, vers, is_file, [], {}, node_instance)
    # Output_path variable contains the path of the generated
    # XML records that will be indexed in Solr.
    # Test the operation intended by the user from the input.
    if operation == PUBLISH_OP:
        output_path = extract_metadata(fields_dictionary, dataset_instance.path, output_dir, dataset_instance,
                                       node_instance)
        print(output_path)
    elif operation == UNPUBLISH_OP:
        output_path = path
    else:
        print("Please specify the operation intended.")

    # This url depends on the type of operation chosen by the user through the initial input.
    operation_url = config.get('generic', session.operation)
    index(output_path, cert_file, header, operation_url)


if __name__ == "__main__":
    main()
