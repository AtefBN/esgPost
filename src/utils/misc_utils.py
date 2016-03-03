import os
import subprocess
from constants import *
from custom_exceptions import *
from lxml import etree
from extract import *
import shutil
import hashlib


def check_version(v):
    """
    :param v: vers input
    :return: Boolean.
    """
    try:
        return True
    except ValueError as msg:
        raise InvalidVersionNumber


def check_path(path, xml_input):
    """
    Checks whether the input path is a valid path to a directory or file or not
    Reinforced with regex and os.path
    The couple booleans (is_file, valid_path) help determine 3 states : the path is a file, the path is a directory
    or the path is not valid. (True, True)=file, (False, True)=directory, (False, False)=Not a valid path.
    :param path: String
    :param xml_input: boolean indicating whether the publication is via xml or netcdf
    :return: Boolean couple.
    """
    # This variable supposes the path is not a file.
    # It could be either a directory or not a valid path at all.
    is_file = False
    if os.path.isfile(path):
        valid_path = True
        is_file = True

    elif os.path.isdir(path):
        number_of_files, valid_path, path = handle_directory(path, xml_input)
        if number_of_files == 1:
            is_file = True
    else:
        raise InvalidPathException
    return is_file, valid_path, os.path.realpath(path)


def create_query(cert, data, header, wsurl):
    curl_query = "curl --key " + cert + "  --cert " + cert + \
                         " --verbose -X POST -d @" + data + " --header " + header + \
                         " " + wsurl
    print('The generated query is ' + curl_query)
    return curl_query


def index(output_path, unpublish_file, certificate_file, header_form, session):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via Pycurl, however for the
    time being we just use a direct curl command.
    :param output_path: String
    :param certificate_file : path to the certificate_file
    :param header_form : contains header information
    :param session : a Session instance carrying information relative to this instance.
    :param unpublish_file : the directory in which the unpublish file resides.
    :return: Success or failure message: String
    """
    # if this is a publishing operation we loop over the xml records and publish them one by one.
    FNULL = open(os.devnull, 'w')
    if session.operation == PUBLISH_OP:
        os.chdir(output_path)
        file_list = os.listdir(output_path)
        # loop over the records generated
        for record in file_list:
            # generate path to each file
            record_path = os.path.join(output_path, record)
            curl_query = create_query(certificate_file, record_path, header_form, session.ws_url)
            proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            print "Curl output: %s, errors: %s" % (out, err)

    # in case of unpublish operation, we generate the id only.
    elif session.operation == UNPUBLISH_OP:
        curl_query = create_query(certificate_file, unpublish_file, header_form, session.ws_url)
        # TODO uncomment this.
        shutil.rmtree(unpublish_file)
        proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        print "Curl output: %s, errors: %s" % (out, err)

    # Either case, execute query and print outcome.


def convert_path_to_drs(path):
    """
    :param path: string in format /path/to/directory/or/file
    :return: path.to.directory.or.file
    """
    path = os.path.abspath(path)
    base_id = ''
    for index_num, c in enumerate(path):
            # skipping the slashes '/' from start and end of the path string.
            if (index_num == 0 or index_num == len(path) - 1) and c == SLASH:
                continue
            # building the base which coincides with the DRS id.
            elif c != SLASH:
                base_id += c
            # replacing the inner slashes with dots.
            else:
                base_id += DOT
    return base_id


def validate_xml(path, drs_dict):
    """
    The method compares the contents of the xml file to the DRS structure extracted info. In case of lacking information
    they are added to the xml record.
    :param path: path to xml record
    :param drs_dict : dictionary containing parameters harvested from DRS.
    :return: modified xml file
    """
    index = 0
    # if os.path.isfile(path):
    index += 1
    print(index)
    tree = etree.parse(path)
    root = tree.getroot()
    list_of_keys = []
    for child in tree.iter('*'):
        for key, value in child.items():
            list_of_keys.append(value)
    print(list_of_keys)
    for key in drs_dict.keys():
        print(key)
        if key not in list_of_keys and key != IGNORE_STR:
            print('This key %s was not found in the record and will be added from DRS' % key)
            append_to_xml(root, key, drs_dict[key])
            etree.tostring(tree, pretty_print=True)
            print('The record has been enriched')
    return tree
    # else:
    #     raise NoFilesFound


def append_to_xml(root, key, value):
    """
    Adds a sub-element to a root element of an existing xml file or one in building.
    :param root: Root element of the xml document
    :param key: the key attribute for the tag
    :param value: the value that will be inserted to the text
    """
    new_elt = etree.SubElement(root, FIELD, name=key)
    try:
        new_elt.text = str(value)
    except Exception:
        pass


def handle_directory(path, xml_input):
    """
    This function explores given path and finds how many files are within (netCDF or xml)
    :param path:
    :param xml_input:
    :return:
    """
    print('received this path %s and this xml_input' % path, xml_input)
    if xml_input:
        extension = XML_EXTENSION
    else:
        extension = NC_EXTENSION
    valid_path = os.path.isdir(path)
    print('valid path %s' % valid_path)
    os.chdir(path)
    file_list = os.walk(path)
    number_of_files = 0
    for triplet in file_list:
        for file_name in triplet[2]:
            if file_name.endswith(extension):
                number_of_files += 1
                # Found a netCDF file, useful if directory contains only one file.
                persistent_file_name = file_name
    if number_of_files == 0:
        raise NoFilesFound
    elif number_of_files == 1:
        is_file = True
        # Adding the filename to the path to get a file instead of a directory.
        path = os.path.join(path, persistent_file_name)
    return number_of_files, valid_path, path


def create_output_dir(drs_id, output_parent):
    """
    This function builds the output path out of the DRS id, to ensure proper access and organization of the records.
    :param drs_id: id containing the details of the dataset.
    :param output_parent: base of the output directory.heaven
    :return: the absolute path of the output.
    """
    if os.path.isdir(output_parent):
        os.chdir(output_parent)
    else:
        raise OutputDirectoryNotFound
    dir_list = drs_id.split(DOT)
    for directory in dir_list:
        if not os.path.exists(directory):
            print 'creating the following directory %s' % directory
            os.mkdir(directory)
        os.chdir(directory)
        output_parent = os.path.join(output_parent, directory)
    return output_parent


def create_unpublish_xml(unpublish_dir, node_instance, path):
    gen_id = unpublish_id(path, node_instance)
    page = etree.Element(DOC)
    doc = etree.ElementTree(page)
    new_elt = etree.SubElement(page, FIELD, name=ID)
    new_elt.text = str(gen_id)
    unpub_file = os.path.join(unpublish_dir, UNPUBLISHING_FILE)
    out_file = open(unpub_file, 'w')
    # Writing the dataset main record.
    doc.write(out_file, pretty_print=True)
    return unpub_file


def get_size(path):
    """
    :param path: string containing the path you wish to get the size of.
    :return the size of the directory/file indicated by the path.
    """
    result = 0
    for root, dirs, files in os.walk(path):
        result += sum(os.path.getsize(os.path.join(root, name)) for name in files)
    return result


def hash_file(a_file, hash_technique, block_size=65536):
    buf = a_file.read(block_size)
    while len(buf) > 0:
        hash_technique.update(buf)
        buf = a_file.read(block_size)
    return hash_technique.hexdigest()


print(hash_file(open("/home/abennasser/Images/injuries.png", 'rb'), hashlib.md5()))


