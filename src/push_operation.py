import getopt
import sys
import ConfigParser

from scan_and_extract import extract_from_file
from ws_curl import index

usage = """
schema
masterid
instance_id
file_type : Dataset or file
dataset_id :  the dataset id.
id : the id of the data to be published
version : Select the version of the file to be published
unpublish, publish : Select either publish or unpublish. But exclusively one at the time. Otherwise the algorithm will select the
latest input.
mapfile : precise the location of the already generated mapfile of the dataset you wish to unpublish.
"""
Config = ConfigParser.ConfigParser()
print(Config.read('/root/PycharmProjects/esgPost/src/test.ini'))
cert_file = Config.get('generic', 'certificate_file')
header = Config.get('generic', 'header')
temp_dir = Config.get('generic', 'temp_dir')
index_node = Config.get('generic', 'index_node')
data_node = Config.get('generic', 'data_node')
ws_post_url = Config.get('generic', 'ws_publish')
PUBLISH_OP = "ws_publish"
UNPUBLISH_OP = "ws_unpublish"


def main():
    argv = sys.argv[1:]
    mandatory_options_dict = {}
    path = '/home/abennasser/'
    operation = PUBLISH_OP
    mapfile = None
    try:
        args, last_args = getopt.getopt(argv, "", ["help", "schema=", "masterid=", "instance_id=", "file_type=",
                                                   "dataset_id=", "id=", "--path", "version=", "publish", "unpublish",
                                                   "mapfile="])

    except getopt.error:
        print sys.exc_value
        print usage
        sys.exit(0)
    for o, a in args:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            print(usage)
            sys.exit()
        elif o in ("-s", "--schema"):
            mandatory_options_dict['schema'] = a
        elif o in ("-m", "--masterid"):
            mandatory_options_dict['master_id'] = a
        elif o == "--instance_id":
            mandatory_options_dict['instance_id'] = a
        elif o == "--file_type":
            mandatory_options_dict['type'] = a
        elif o == "--dataset_id":
            mandatory_options_dict['dataset_id'] = a
        elif o == "--version":
            mandatory_options_dict['version'] = a
        elif o == "--id":
            mandatory_options_dict['id'] = a
        elif o == "--path":
            mandatory_options_dict['path'] = a
        elif o == "--unpublish":
            operation = UNPUBLISH_OP
        elif o == "--publish":
            operation = PUBLISH_OP
        elif o == "--mapfile":
            mapfile = a
        else:
            assert False, "unhandled option"
    # Result path variable contains the path of the generated
    # XML descriptor that will be pushed to solr for indexation
    if operation == "ws_publish":
        result_path = extract_from_file(mandatory_options_dict, path, temp_dir)
    elif operation == "ws_unpublish":
        result_path = mapfile
    else:
        print("Please specify the operation.")
    # This url depends on the type of operation chosen via the options.
    ws_post_url = Config.get('generic', operation)
    index(result_path, cert_file, header, ws_post_url)


if __name__ == "__main__":
    main()