import netCDF4

from lxml import etree

from utils import *

from pprint import pprint

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

    # page = etree.Element('doc')
    # doc = etree.ElementTree(page)
    # WTF is this line of code ?
    output_path = output_dir

    # Filling up the mandatory options from dictionary
    # and printing them into an XML file
    # fields_dictionary['drs_id'], fields_dictionary['dataset_id'], \
    #    fields_dictionary['master_id'], fields_dictionary['id'], \
    #    fields_dictionary['instance_id'] = extract_ids(dataset.path, dataset.file_name, dataset.vers, \
    #    node.data_node,dataset.is_file)
    # fields_dictionary['index_node'] = node.index_node
    # fields_dictionary['data_node'] = node.data_node
    # for key, value in fields_dictionary.iteritems():
    #    new_elt = etree.SubElement(page, 'field', name=key)
    #    new_elt.text = value
    # Adding the url to the fileServer using the output_path as
    # server path to the posted file.

    # TODO inspect the URL ENTRY for generated XML records.
    # new_elt = etree.SubElement(page, 'field', name="url")
    # new_elt.text = output_path + "|application/netcdf|Fileserver"

    # Scanning path for netcdf files
    # and filling up XML descriptive file
    scan_directory(dataset, node)
    # Preparing the dataset's folder:
    dataset_folder = output_path + slash + dataset.id_dictionary[DRS]
    if not os.path.exists(dataset_folder):
        os.makedirs(dataset_folder)
    # Writing records in the output directory:
    out_file = open(dataset_folder + slash + "dataset" + dot + dataset.id_dictionary[DRS] + xml_extension, 'w')
    dataset.record.write(out_file, pretty_print=True)
    # Writing each file record in the appropriate record file:
    for netcdf_file in dataset.netCDFFiles:
        out_file = open(dataset_folder + slash + netcdf_file.id_dictionary[DRS] + dot + netcdf_file.file_name + xml_extension, 'w')
        netcdf_file.record.write(out_file, pretty_print=True)

    print("Successfully generated records and wrote in the parent output "
          "directory " + output_path + " under the dataset folder " + dataset_folder)
    return dataset_folder


def scan_directory(dataset, node):
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
        dataset.generate_dataset_record(node)
    # in case multiple files are under within the dataset:
    else:
        os.chdir(dataset.path)
        file_list = os.listdir(dataset.path)
        for file_name in file_list:
            netcdf_file = scan_single_netcdf_file(dataset.path, file_name, dataset, node)
            dataset.number_of_files += 1
        dataset.generate_dataset_record(node)


def scan_single_netcdf_file(path, file_name, dataset_instance, node_instance):
    """
    This method scans a single netcdf file, generates the XML record and returns the netcdf file object,
    with record attribute filled.
    :param path : String path to file
    :param file_name : String the file name
    :param dataset_instance : a dataset_instance instance in which this file belongs
    :param node_instance : the node_instance instance

    :return netcdf file object instance
    """
    print("TEST "+path)
    if path.endswith(slash):
        path_to_file = path + file_name
    else:
        path_to_file = path + slash + file_name
    print("TEST 2 "+path_to_file)
    open_netcdf_file = netCDF4.Dataset(path_to_file, 'r')
    # None value corresponds to the xml record for this file, it will be generated afterwards.
    open_netcdf_file.ncattrs()
    open_netcdf_file.variables.keys()
    netcdf_file = NetCDFFile(path_to_file, open_netcdf_file.variables.keys(), open_netcdf_file.ncattrs(), dataset_instance, node_instance)
    # Generating the record.
    netcdf_file.generate_record(open_netcdf_file, dataset_instance, node_instance)
    # Add the single file to the list of netCDF files of the dataset_instance.
    dataset_instance.netCDFFiles.append(netcdf_file)
    return netcdf_file

