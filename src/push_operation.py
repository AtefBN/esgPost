import getopt
import sys
import ConfigParser
import shutil
from scan_and_extract import extract_metadata
from utils import *
from models import *
from custom_exceptions import *

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
config.read('/root/PycharmProjects/esgPost/src/misc.ini')
# config.read('/home/esg-user/esgPost/misc.ini')
cert_file = config.get('generic', 'certificate_file')
header = config.get('generic', 'header')
output_dir = config.get('generic', 'output_dir')
index_node = config.get('generic', 'index_node')
data_node = config.get('generic', 'data_node')
ws_post_url = config.get('generic', 'ws_publish')
unpublish_dir = config.get('generic', 'unpublish_dir')
# Create the datanode options with the appropriate options
node_instance = Node(data_node, index_node, cert_file, header)


def main():
    remove_records = False
    gen_id = ''
    output_path = ''
    argv = sys.argv[1:]
    fields_dictionary = {}
    # Start harvesting user input.
    try:
        args, last_args = getopt.getopt(argv, "",
                                        ["help", "schema=", "path=", "version_number=", "publish", "unpublish",
                                         "remove_records"])
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
        elif o == "--version_number":
            version_number = a
        elif o == "--path":
            is_file, valid_path, path = check_path(a)
            if not valid_path:
                raise InvalidPathException
        elif o == "--unpublish":
            operation = UNPUBLISH_OP
        elif o == "--publish":
            operation = PUBLISH_OP
        elif o == "--remove_records":
            remove_records = True
        else:
            assert False, "unhandled option"
    # Based on input create the Dataset that will host the netCDF files as well as the node_instance.
    # The empty lists are fillers for coming attributes of the dataset,
    # namely attributes coming from files.
    operation_url = config.get('generic', operation)
    session = Session(operation, operation_url)

    # Testing inputs to avoid crash later.
    if session.operation == PUBLISH_OP and\
            not ('schema' in vars().keys() and 'version_number' in vars().keys() and 'path' in vars().keys()):
        raise BadSetOfOptions('The options in the input are incomplete, check the help.', 1)
    elif session.operation == UNPUBLISH_OP and 'path' not in vars().keys() and 'version_number' not in vars().keys():
        raise BadSetOfOptions('The options in the input are incomplete, check the help', 1)

    # Test the operation intended by the user from the input.
    if operation == PUBLISH_OP:
        # initiating a dataset instance according to the user's input
        dataset_instance = Dataset(path, schema, version_number, is_file, [], {}, node_instance)
        # Output_path variable contains the path of the generated
        # XML records that will be indexed in Solr.
        output_path = extract_metadata(fields_dictionary, dataset_instance.path, output_dir, dataset_instance,
                                       node_instance)
    elif operation == UNPUBLISH_OP:
        # Generating the id used for unpublishing.
        gen_id = unpublish_id(path, version_number, node_instance)
        page = etree.Element('doc')
        doc = etree.ElementTree(page)
        new_elt = etree.SubElement(page, 'field', name='id')
        new_elt.text = str(gen_id)
        out_file = open(unpublish_dir + xml_extension, 'w')
        # Writing the dataset main record.
        doc.write(out_file, pretty_print=True)

    else:
        raise NoOperationSelected('Please select an operation to perform, either publish or unpublish. See the the'
                                  'help for further details.')

    # Push the generated records or the generated id.
    index(output_path, unpublish_dir, cert_file, header, session)

    if remove_records and operation == PUBLISH_OP:
            # Go up one level to delete the output directory
            shutil.rmtree(output_path)

if __name__ == "__main__":
    main()
