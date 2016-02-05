import os
import re
import subprocess
from constants import *
from custom_exceptions import *
import ConfigParser
from lxml import etree


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
    if check_file.match(path):
        valid_path = os.path.isfile(path)
        is_file = True
    elif check_directory.match(path):
        valid_path = os.path.isdir(path)
        os.chdir(path)
        file_list = os.walk(path)
        number_of_files = 0
        for triplet in file_list:
            for file_name in triplet[2]:
                if file_name.endswith('.nc'):
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


def create_query(cert, data, header, wsurl):
    curl_query = "curl --key " + cert + "  --cert " + cert + \
                         " --verbose -X POST -d @" + data + " --header " + header + \
                         " " + wsurl
    print('The generated query is ' + curl_query)
    return curl_query


def index(output_path, unpublish_dir, certificate_file, header_form, session):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via Pycurl, however for the
    time being we just use a direct curl command.
    :param output_path: String
    :param certificate_file : path to the certificate_file
    :param header_form : contains header information
    :param session : a Session instance carrying information relative to this instance.
    :param unpublish_dir : the directory in which the unpublish file resides.
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
            if output_path.endswith(SLASH):
                record_path = output_path + record
            else:
                record_path = output_path + SLASH + record
            curl_query = create_query(certificate_file, record_path, header_form, session.ws_url)
            proc = subprocess.Popen([curl_query], stdout=FNULL, stderr=subprocess.STDOUT, shell=True)
            (out, err) = proc.communicate()
            print "Curl output: %s, errors: %s" % (out, err)

    # in case of unpublish operation, we generate the id only.
    elif session.operation == UNPUBLISH_OP:
        curl_query = create_query(certificate_file, unpublish_dir, header_form, session.ws_url)
        # shutil.rmtree(unpublish_dir)
        proc = subprocess.Popen([curl_query], stdout=FNULL, stderr=subprocess.STDOUT, shell=True)
        (out, err) = proc.communicate()
        print "Curl output: %s, errors: %s" % (out, err)

    # Either case, execute query and print outcome.


def convert_path_to_drs(path):
    """
    :param path: string in format /path/to/directory/or/file
    :return: path.to.directory.or.file
    """
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


# TODO investigate if this method is possible
def read_config(path, section, key):
    config = ConfigParser.ConfigParser()
    config.read(path)
    value = config.get(section, key)
    return value

def check_xml(path):
    """
    The method compares the contents of the xml file to the DRS structure extracted info. In case of lacking information
    they are added to the xml record.
    :param path: path to xml record
    :return: modified xml file
    """
    tree = etree.parse(path)
    root = etree.tostring(tree.getroot())
    # print(root)
    print tree.xpath("//field[@name=variable]/content/text()")



check_xml('/home/abennasser/output/home.abennasse/Dataset.home.abennasse.xml')