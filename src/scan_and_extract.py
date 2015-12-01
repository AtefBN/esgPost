__author__ = 'root'

import glob
import os

from lxml import etree
from Scientific.IO import NetCDF


def extract_from_file(mandatory_options, path, temp_dir):
    """
    Fills up the XML file to be indexed in solr via PUSH operation.
    It takes into consideration two types of params: mandatory params and harvested params.
    The mandatory params are fed through the ini file and/or options.
    The harvested data is found after exploration of the netcdf files exposed.
    """
    page = etree.Element('doc')
    doc = etree.ElementTree(page)
    result_path = temp_dir
    # Filling up the mandatory options from dictionary
    # and printing them into an XML file
    for key, value in mandatory_options.iteritems():
        print("processing...", key, value)
        new_elt = etree.SubElement(page, 'field', name=key)
        new_elt.text = value
    # Scanning path for netcdf files
    # and filling up XML descriptive file
    scan_directory(path, page)
    print('test' + result_path)
    out_file = open(result_path, 'w')
    doc.write(out_file)
    print("Successfully wrote in the file " + result_path)
    return result_path


def scan_directory(my_path, xml_page):
    """
    Given a dataset parent path, this method explores one level
    of depth and converts the netcdf files found into a descriptive
    XML file with
    :param my_path: String
    :return: modified xml descriptive page
    """
    os.chdir(my_path)
    for file_name in glob.glob("*.nc"):
        file_object = NetCDF.NetCDFFile(my_path + "/" + file_name, "r")
        global_att_list = dir(file_object)
        # Extracting attributes
        for global_attr in global_att_list:
            global_attr_value = getattr(file_object, global_attr)
            if global_attr != "project_id":
                new_elt = etree.SubElement(xml_page, 'field', name=global_attr)
            else:
                new_elt = etree.SubElement(xml_page, 'field', name="project")
            if isinstance(global_attr_value, basestring):
                new_elt.text = global_attr_value
        all_var_names = file_object.variables.keys()
        for var in all_var_names:
            new_elt = etree.SubElement(xml_page, 'field', name="variable")
            new_elt.text = var
        file_object.close()
    return xml_page
