import getopt
import shutil
import sys
sys.path.append('/root/PycharmProjects/esgPost')
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
config.read('/root/PycharmProjects/esgPost/misc.ini')
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
                                        ["help", "schema=", "path=", "version_number=", "publish", "unpublish",
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
        elif o == "--xml_input":
            xml_input = True
        else:
            assert False, "unhandled option"
    # Based on input create the Dataset that will host the netCDF files as well as the node_instance.
    # The empty lists are fillers for coming attributes of the dataset,
    # namely attributes coming from files.
    operation_url = config.get('utils-webservice', operation)
    session = Session(operation, operation_url)

    # Testing inputs to avoid crash later.
    # if session.operation == PUBLISH_OP and\
    #         not ('schema' in vars().keys() and 'version_number' in vars().keys() and 'path' in vars().keys()):
    #     raise BadSetOfOptions('The options in the input are incomplete, check the help.', 1)
    # elif session.operation == UNPUBLISH_OP and 'path' not in vars().keys() and 'version_number' not in vars().keys():
    #     raise BadSetOfOptions('The options in the input are incomplete, check the help', 1)

    # Test the operation intended by the user from the input.
    if operation == PUBLISH_OP:
        if not xml_input:
            # initiating a dataset instance according to the user's input
            dataset_instance = Dataset(path, schema, version_number, is_file, [], {}, node_instance)
            # Output_path variable contains the path of the generated
            # XML records that will be indexed in Solr.
            output_path = extract_metadata(output_dir, dataset_instance,
                                           node_instance)
        else:
            # The data that will be published is already parsed in an xml format and is ready to be published
            # in this case the push mode publisher directly points towards the directory of the xml files
            # as if they were generated from netCDF files.
            drs_dict = extract_from_drs(os.path.abspath(path))
            check_xml(path)
            output_path = path
    elif operation == UNPUBLISH_OP:
        # Generating the id used for unpublishing.
        gen_id = unpublish_id(path, version_number, node_instance)
        page = etree.Element('doc')
        doc = etree.ElementTree(page)
        new_elt = etree.SubElement(page, 'field', name='id')
        new_elt.text = str(gen_id)
        unpub_file = os.path.join(unpublish_dir, 'unpub.xml')
        out_file = open(unpub_file, 'w')
        # Writing the dataset main record.
        doc.write(out_file, pretty_print=True)

    else:
        raise NoOperationSelected('Please select an operation to perform, either publish or unpublish. See the the'
                                  'help for further details.')

    # Push the generated records or the generated id.
    # index(output_path, unpub_file, cert_file, header, session)

    if remove_records and operation == PUBLISH_OP:
            # Go up one level to delete the output directory
            shutil.rmtree(output_path)
    time_elapsed = time() - tic
    print("The publishing process took " + str(time_elapsed) + ' s')
if __name__ == "__main__":
    main()
