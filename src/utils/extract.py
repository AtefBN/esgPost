import ConfigParser
import netCDF4
import os
import constants
from constants import *

from src.models.models import *


def extract_metadata(output_dir, dataset, node, drs_dict):
    """
    Fills up the XML file to be indexed in solr via PUSH operation.
    It takes into consideration two types of params: mandatory params and harvested params.
    The mandatory params are fed through the ini file and/or options.
    The harvested data is found after exploration of the netcdf files exposed.
    :param output_dir : String indicating path of the directory where the xml file will be stored.
    :param dataset : dataset instance
    :param node : node instance

    :return: output_path : String indicating where the xml file is located.
    """
    # output_path is a string that will be built based on the output directory
    output_path = output_dir
    scan_directory(dataset, node, drs_dict)

    # Preparing the dataset's folder:
    dataset_folder = create_output_dir(dataset.id_dictionary[DRS_ID], output_path)

    # Writing records in the output directory:
    out_file = open(dataset_folder + SLASH + DATASET + DOT + dataset.id_dictionary[DRS_ID] + XML_EXTENSION, 'w')

    # Writing the dataset main record.
    dataset.record.write(out_file, pretty_print=True)

    # Writing each file record in the appropriate record file:
    for netcdf_file in dataset.netCDFFiles:
        out_file = open(dataset_folder + SLASH + FILE + DOT + netcdf_file.id_dictionary[DRS_ID] + DOT +
                        netcdf_file.file_name + XML_EXTENSION, 'w')
        # Write every netcdf's file appropriate XML record.
        netcdf_file.record.write(out_file, pretty_print=True)

    print("Successfully generated %s" % str(dataset.number_of_files+1) + " records and wrote in the parent output "
          "directory " + output_path + " under the dataset folder " + dataset_folder)
    return dataset_folder


def scan_directory(dataset, node, drs_dict):
    """
    Given a dataset parent path, this method explores one level
    of depth and harvests the metadata of the netcdf files found.
    The result is a descriptive XML file.
    :param dataset: dataset instance
    :param node: node instance

    :return: modified xml descriptive page ready to be pushed to solr.
    """
    # in case of a single file within the dataset:
    if dataset.is_file:
        dataset.number_of_files = 1
        # the two none value are fillers for later attributes resulting from exploring the netcdf file.
        netcdf_file = scan_single_netcdf_file(dataset.path, dataset.file_name, dataset, node, drs_dict)
        dataset.generate_dataset_record(node)
    # in case multiple files are under within the dataset:
    else:

        # os.chdir(dataset.path)
        file_list = os.walk(dataset.path)
        for f in file_list:
            for file_name in f[2]:
                if file_name.endswith(NC_EXTENSION):
                    path_to_file = os.path.join(dataset.path, f[0])
                    netcdf_file = scan_single_netcdf_file(path_to_file, file_name, dataset, node, drs_dict)
                    dataset.number_of_files += 1
        dataset.generate_dataset_record(node)


def scan_single_netcdf_file(path, file_name, dataset_instance, node_instance, drs_dict):
    """
    This method scans a single netcdf file, generates the XML record and returns the netcdf file object,
    with record attribute filled.
    :param path : String path to file
    :param file_name : String the file name
    :param dataset_instance : a dataset_instance instance in which this file belongs
    :param node_instance : the node_instance instance

    :return netcdf file object instance
    """
    path_to_file = os.path.join(path, file_name)
    print('extracting metadata from this file '+path_to_file)
    open_netcdf_file = netCDF4.Dataset(path_to_file, 'r')
    open_netcdf_file.ncattrs()
    open_netcdf_file.variables.keys()
    netcdf_file = NetCDFFile(path_to_file, open_netcdf_file.variables.keys(), open_netcdf_file.ncattrs(),
                             dataset_instance, node_instance)
    # Generating the record.
    netcdf_file.generate_record(open_netcdf_file, dataset_instance, node_instance, drs_dict)
    # Add the single file to the list of netCDF files of the dataset_instance.
    dataset_instance.netCDFFiles.append(netcdf_file)
    return netcdf_file


def extract_from_drs(absolute_path):
    """
    :param absolute_path: String contains the absolute path to the dataset.
    :return: dictionary of extracted params
    """
    # Verifying the path is absolute
    absolute_path = os.path.abspath(absolute_path)
    # Initializing output dictionary.
    param_dic = dict()

    # Preparing the list of drs values.
    drs_list = absolute_path.split(SLASH)
    for element in drs_list:
        # if the path starts with a slash it will generate an empty string at first.
        if element == '':
            drs_list.remove(element)

    # Getting the DRS configuration for keys.
    config = ConfigParser.ConfigParser()
    config.read(PATH_TO_CONFIG)
    drs_elements_list = config.get('utils-generic', 'DRS_elements_list').split(SLASH)
    list_index = 0
    for drs_key in drs_elements_list:
        # Added length control on list to avoid exceptions
        if len(drs_list) > list_index:
            param_dic[drs_key] = drs_list[list_index]
            list_index += 1
        else:
            raise IncompatibleWithDRSConfigPath()

    # in case of latest in the version section we get the symbolic link destination.
    if param_dic[VERSION] == LATEST_STR:
        param_dic[VERSION] = os.path.basename(os.path.realpath(absolute_path)).replace(VERSION_STR, '')
    return param_dic


def unpublish_id(path, node):
    """
    :param path
    :param version
    :param node
    this method generate an ID like the following:
    '<delete><query>id:cmip5.test.v1|esgf-dev.dkrz.de</query></delete>'
    """
    abs_path = os.path.abspath(path)
    drs_dict = extract_from_drs(os.path.abspath(abs_path))
    base_id = convert_path_to_drs(abs_path)
    gen_id = base_id + DOT + VERSION_STR + drs_dict[VERSION] + PIPE + node.data_node
    return gen_id
