import getopt
import sys
import ConfigParser

from scan_and_extract import extract_from_file
from ws_curl import index

usage = """
schema
masterid
instance_id
file_type
dataset_id
id
version
"""
Config = ConfigParser.ConfigParser()
print(Config.read("/root/PycharmProjects/esgPost/src/test.ini"))
cert_file = Config.get("generic", 'certificate_file')
header = Config.get('generic', 'header')
temp_dir = Config.get('generic', 'temp_dir')
ws_post_url = Config.get('generic', 'ws_publish')
index_node = Config.get('generic', 'index_node')
data_node = Config.get('generic', 'data_node')
print('This is a test ' + header)


def main():
    argv = sys.argv[1:]
    mandatory_options_dict = {}
    path = "/home/abennasser/"

    try:
        args, lastargs = getopt.getopt(argv, "", ["help", "schema=", "masterid=", "instance_id=", "file_type=",
                                                  "dataset_id=", "id", "version"])

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
            print("HERE IS THE VERSION", a)
            mandatory_options_dict['version'] = a
        elif o == "--id":
            mandatory_options_dict['id'] = a
        elif o == "--path":
            mandatory_options_dict['path'] = a
        else:
            assert False, "unhandled option"
    for key, value in mandatory_options_dict.iteritems():
        print(key, value)
    # Result path variable contains the path of the generated
    # XML descriptor that will be pushed to solr for indexation
    result_path = extract_from_file(mandatory_options_dict, path, temp_dir)
    index(result_path, cert_file, header, ws_post_url)


if __name__ == "__main__":
    main()
