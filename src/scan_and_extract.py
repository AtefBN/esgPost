import netCDF4

from lxml import etree

from utils import *

from models import *


def extract_metadata(fields_dictionary, path, output_dir, dataset, node):
    """
    Fills up the XML file to be indexed in solr via PUSH operation.
    It takes into consideration two types of params: mandatory params and harvested params.
    The mandatory params are fed through the ini file and/or options.
    The harvested data is found after exploration of the netcdf files exposed.
    :param fields_dictionary: Dictionary of mandatory fields.
    :param path: String indicating path of dataset.
    :param output_dir: String indicating path of the directory where the xml file will be stored.
    :return: output_path : String indicating where the xml file is located.
    """

    page = etree.Element('doc')
    doc = etree.ElementTree(page)
    # WTF is this line of code ?
    output_path = output_dir

    # Filling up the mandatory options from dictionary
    # and printing them into an XML file
    # fields_dictionary['drs_id'], fields_dictionary['dataset_id'], \
    #    fields_dictionary['master_id'], fields_dictionary['id'], \
    #    fields_dictionary['instance_id'] = extract_ids(dataset.path, dataset.file_name, dataset.vers, \
    #    node.data_node,dataset.is_file)
    fields_dictionary['index_node'] = node.index_node
    fields_dictionary['data_node'] = node.data_node
    for key, value in fields_dictionary.iteritems():
        new_elt = etree.SubElement(page, 'field', name=key)
        new_elt.text = value
    # Adding the url to the fileServer using the output_path as
    # server path to the posted file.

    # TODO inspect the URL ENTRY for generated XML records.
    # new_elt = etree.SubElement(page, 'field', name="url")
    # new_elt.text = output_path + "|application/netcdf|Fileserver"

    # Scanning path for netcdf files
    # and filling up XML descriptive file
    scan_directory(path, page)
    out_file = open(output_path, 'w')
    doc.write(out_file)

    print("Successfully generated records and wrote in the file " + output_path)
    return output_path, doc


def scan_directory(path, dataset, node):
    """
    Given a dataset parent path, this method explores one level
    of depth and harvests the metadata of the netcdf files found.
    The result is a descriptive XML file.
    :param path: String
    :return: modified xml descriptive page ready to be pushed to solr.
    """
    # in case of a single file within the dataset:
    if dataset.is_file:
        dataset.number_of_files = 1
        # the two none value are fillers for later attributes resulting from exploring the netcdf file.
        netcdf_file = scan_single_netcdf_file(dataset.path, dataset.file_name, dataset, node)
        edit_file_record(netcdf_file)
        edit_dataset_record(dataset)
    # in case multiple files are under within the dataset:
    else:
        os.chdir(path)
        file_list = os.listdir(path)
        for file_name in file_list:
            netcdf_file = scan_single_netcdf_file(dataset.path, dataset.file_name, dataset, node)
            dataset.number_of_files += 1
            edit_file_record(netcdf_file)
        edit_dataset_record(dataset)


def scan_single_netcdf_file(path, file_name, dataset, node):
    open_file = netCDF4.Dataset(path + '/' + file_name, 'r')
    netcdf_file = NetCDFFile(path, open_file.ncattrs(), open_file.variables.keys(), dataset, node)
    # Add the single file to the list of netCDF files of the dataset.
    dataset.netCDFFiles.append(netcdf_file)
    return netcdf_file


def edit_file_record(netcdf_file):
    pass


def edit_dataset_record(dataset_instance):
    pass

"""
    if os.path.isdir(path):
        os.chdir(path)
        file_list = os.listdir(path)
    else:
        # create one element list.
        file_list = [path]
    for netcdf_file_name in file_list:
        # in case the directory is a dataset of netcdf files.
        if ".nc" in netcdf_file_name:
            netcdf_file = netCDF4.Dataset(path + "/" + netcdf_file_name, "r")
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
    """