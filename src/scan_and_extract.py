import netCDF4

from lxml import etree

from utils import *


def extract_from_file(mandatory_options, path, temp_dir, index_node, data_node, version, isFile, isDir):
    """
    Fills up the XML file to be indexed in solr via PUSH operation.
    It takes into consideration two types of params: mandatory params and harvested params.
    The mandatory params are fed through the ini file and/or options.
    The harvested data is found after exploration of the netcdf files exposed.
    :param mandatory_options: Dictionary of mandatory fields.
    :param path: String indicating path of dataset.
    :param temp_dir: String indicating path of the directory where the xml file will be written.
    :return: result_path : String indicating where the xml file is located.
    """

    file_name, directory = extract_file_name(path)
    page = etree.Element('doc')
    doc = etree.ElementTree(page)
    result_path = temp_dir
    # Filling up the mandatory options from dictionary
    # and printing them into an XML file
    mandatory_options['drs_id'], mandatory_options['dataset_id'], \
    mandatory_options['master_id'], mandatory_options['id'], \
    mandatory_options['instance_id'] = extract_ids(directory, file_name, version, data_node, isFile, isDir)
    mandatory_options['index_node'] = index_node
    mandatory_options['data_node'] = data_node
    for key, value in mandatory_options.iteritems():
        new_elt = etree.SubElement(page, 'field', name=key)
        new_elt.text = value
    # Adding the url to the fileServer using the result_path as
    # server path to the posted file.

    # TODO inspect the URL ENTRY.
    # new_elt = etree.SubElement(page, 'field', name="url")
    # new_elt.text = result_path + "|application/netcdf|Fileserver"

    # Scanning path for netcdf files
    # and filling up XML descriptive file
    scan_directory(path, page)
    out_file = open(result_path, 'w')
    doc.write(out_file)
    print("Successfully wrote in the file " + result_path)
    return result_path, doc


def scan_directory(my_path, xml_page):
    """
    Given a dataset parent path, this method explores one level
    of depth and harvests the metadata of the netcdf files found.
    The result is a descriptive XML file.
    :param my_path: String
    :return: modified xml descriptive page ready to be pushed to solr.
    """
    # It has been noticed that in multi-files datasets, the dataset_id entry is dropped.
    include_dataset_id = True
    if os.path.isdir(my_path):
        os.chdir(my_path)
        file_list = os.listdir(my_path)
        include_dataset_id = False
    else:
        # create one element list.
        file_list = [my_path]
    for netcdf_file_name in file_list:
        # in case the directory is a dataset of netcdf files.
        if ".nc" in netcdf_file_name:
            netcdf_file = netCDF4.Dataset(my_path + "/" + netcdf_file_name, "r")
            global_att_list = dir(netcdf_file)
            # Extracting attributes
            #  Special treatment for the project_id the key in the
            # netCDF file is different from the project's schema.
            for global_attr in global_att_list:
                global_attr_value = getattr(netcdf_file, global_attr)
                if global_attr != "project_id":
                    new_elt = etree.SubElement(xml_page, 'field', name=global_attr)
                else:
                    new_elt = etree.SubElement(xml_page, 'field', name="project")
                if isinstance(global_attr_value, basestring):
                    new_elt.text = global_attr_value
            all_var_names = netcdf_file.variables.keys()
            for var in all_var_names:
                new_elt = etree.SubElement(xml_page, 'field', name="variable")
                new_elt.text = var
            netcdf_file.close()
    return xml_page


