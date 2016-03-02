from __future__ import division
import getopt
import shutil
import sys
sys.path.append('/root/PycharmProjects/esgPost')
#sys.path.append('/home/esg-user/esgPost')
from time import time
from src.utils.extract import *
from src.models.models import *
from src.utils.custom_exceptions import *

usage = """ The following script requires a minimum of options to function.
schema : The schema under which the files are to published/unpublished
path : The path to the netCDF file/files (directory containing many files) you wish to publish/unpublish
version_number : The version_number of the file to be published
unpublish/publish : Select either publish or unpublish. But exclusively one at the time. Otherwise the algorithm will
select the latest input.
"""

# Retrieving key values from ini file.
config = ConfigParser.ConfigParser()
config.read(PATH_TO_CONFIG)
# config.read('/home/esg-user/esgPost/misc.ini')
output_dir = config.get('generic', 'output_dir')
index_node = config.get('generic', 'index_node')
data_node = config.get('generic', 'data_node')
unpublish_dir = config.get('generic', 'unpublish_dir')

# Web Service configuration
cert_file = config.get('utils-webservice', 'certificate_file')
header = config.get('utils-webservice', 'header')
ws_post_url = config.get('utils-webservice', 'ws_publish')
# Create the datanode options with the appropriate options
node_instance = Node(data_node, index_node, cert_file, header)


def main():
    tic = time()
    operation = PUBLISH_OP
    remove_records = False
    xml_input = False
    drs_extract = False
    gen_id = ''
    output_path = ''
    unpub_file = ''
    argv = sys.argv[1:]
    try:
        args, last_args = getopt.getopt(argv, "",
                                        ["help", "schema=", "path=", "publish", "unpublish",
                                         "remove_records", "xml_input"])
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
        elif o == "--path":
            path = a
        elif o == "--unpublish":
            operation = UNPUBLISH_OP
        elif o == "--publish":
            operation = PUBLISH_OP
        elif o == "--remove_records":
            remove_records = True
        elif o == "--xml_input":
            xml_input = True
        else:
            assert False, "unhandled option"

    # Testing the input path
    is_file, valid_path, path = check_path(path, xml_input)
    print(valid_path)
    if not valid_path:
        raise InvalidPathException
    # Based on input create the Dataset that will host the netCDF files as well as the node_instance.
    # The empty lists are fillers for coming attributes of the dataset,
    # namely attributes coming from files.
    operation_url = config.get('utils-webservice', operation)
    session = Session(operation, operation_url)
    drs_dict = extract_from_drs(path)
    print('path : %s' % path)
    print(drs_dict)

    # Test the operation intended by the user from the input.
    if operation == PUBLISH_OP:
        if not xml_input:
            # initiating a dataset instance according to the user's input
            dataset_instance = Dataset(path, schema, is_file, [], {}, node_instance, drs_dict)
            # Output_path variable contains the path of the generated
            # XML records that will be indexed in Solr.
            output_path = extract_metadata(output_dir, dataset_instance, node_instance, drs_dict)
        else:
            # The data that will be published is already parsed in an xml format and is ready to be published
            # in this case the push mode publisher directly points towards the directory of the xml files
            # as if they were generated from netCDF files.

            # Go through all the xml files within the directory:
            if os.path.isdir(path):
                file_list = os.walk(path)
                for f in file_list:
                    for file_name in f[2]:
                        if file_name.endswith(XML_EXTENSION):
                            path_to_file = os.path.join(path, file_name)
                            print('Checking the following file %s' % path_to_file)
                            tree = validate_xml(path_to_file, drs_dict)
                            out_file = open(file_name, 'w')
                            tree.write(out_file, pretty_print=True)
                output_path = path
            elif os.path.isfile(path) and path.endswith(XML_EXTENSION):
                tree = validate_xml(path, drs_dict)
                out_file = open(path, 'w')
                tree.write(out_file, pretty_print=True)
                output_path = os.path.dirname(path)
    elif operation == UNPUBLISH_OP:
        # Generating the id used for unpublishing.
        # This is an on-going work, for the moment,
        # The unpublishing operates on a dataset level.
        unpub_file = create_unpublish_xml(unpublish_dir, node_instance, path)
    else:
        raise NoOperationSelected

    # Push the generated records or the generated id.
    index(output_path, unpub_file, cert_file, header, session)

    if remove_records and operation == PUBLISH_OP:
            # Go up one level to delete the output directory
            shutil.rmtree(output_path)
    time_elapsed = time() - tic
    print("The publishing process took " + str(time_elapsed) + ' s for publishing ' + str(dataset_instance.number_of_files) +
          ' files.')

    dataset_size = get_size(path)
    output_size = get_size(output_path)
    print("The dataset size is " + str(dataset_size) + " the output size was reduced to " + str(output_size))
    print("This renders the overall performance equal to regarding size " + str(output_size/dataset_size * 100) + "%")
    print("Regarding time, the process took an overall " + str(time_elapsed/dataset_instance.number_of_files) + ' sec per file')

if __name__ == "__main__":
    main()
