import os
from lxml import etree
from utils import *


class Dataset(object):
    """
    A dataset object refers to an ensemble of files that are intended to be published to solr index.
    Attributes:
        path
        file_type
        vers
        is_file
        file_name
        number_of_files
        netCDFFiles
    """

    def __init__(self, path, schema, version, is_file, netcdf_files, variables, node):
        self.type = 'Dataset'
        self.schema = schema
        self.path = path
        self.version = version
        self.is_file = is_file
        # If the dataset contains, a specific behavior is to be implemented.
        if self.is_file:
            self.file_name, self.path = self.extract_file_name()
        self.number_of_files = 0
        self.netCDFFiles = netcdf_files
        self.variables = variables
        # Initializing Ids
        self.id_dictionary = self.extract_ids(node)
        self.global_attributes = {}
        self.variables = None
        self.record = None
        self.node_info = self.get_node_info(node)

    @staticmethod
    def get_node_info(node):
        node_info = dict()
        node_info['data_node'] = node.data_node
        node_info['index_node'] = node.index_node
        return node_info

    def extract_file_name(self):
        """
        This function is used to extract the file_name from the path if the path is a file path.
        This is used in the sole case of a dataset containing a single file.
        The directory or file test is performed elsewhere.
        :param self.path: string
        :param self.is_file: Boolean

        :return file_name: string, path: string
        """
        # The directory is generated from the path - filename.
        filename = os.path.basename(self.path)
        file_name = filename
        path = self.path.replace(filename, '')
        return file_name, path

    def extract_ids(self, node):
        """
        This method is used to automatically extract the different ids based on the path, vers and filenames.
        :param self: a dataset instance
        :param node: a node instance

        :return id_dictionary : dictionary.
        """
        # Note that the path if this method is solicited is already tested and is a valid path.
        # If this method is to be used elsewhere, make sure to test the path via regex or os lib.

        # DRS_id seems to be the base id.
        # All the other ids can be built from it.
        # Hence I used it as a buffer and base_id.
        base_id = ''

        for index, c in enumerate(self.path):
            # skipping the slashes '/' from start and end of the path string.
            if (self.path.startswith(c) or self.path.endswith(c)) and (index == 0 or index == len(self.path) - 1):
                continue
            # building the base which coincides with the DRS id.
            elif c != slash:
                base_id += c
            # replacing the inner slashes with dots.
            else:
                base_id += dot
        # Building the master and dataset id out of the base id.
        id_dictionary = dict()
        id_dictionary['master_id'] = base_id
        id_dictionary['id'] = base_id + dot + version_str + self.version + pipe + node.data_node
        id_dictionary['instance_id'] = base_id + dot + version_str + self.version
        id_dictionary['drs_id'] = base_id
        return id_dictionary

    def generate_variables(self):
        """
        this function generate the variable list for the dataset from the netCDF file_list.
        """
        variables_set = set()
        for netcdf_file in self.netCDFFiles:
            # replaced by the dictionary structure
            # for global_attr in netcdf_file.global_attributes:
            #    global_attr_set.add(global_attr)
            for variable in netcdf_file.variables:
                variables_set.add(variable)
        self.variables = variables_set
        # Replaced by the dictionary structure
        # self.global_attributes = global_attr_set

    def generate_dataset_record(self, node):
        """
        Function responsible for the generation of the dataset XML record.
        """
        page = etree.Element('doc')
        doc = etree.ElementTree(page)
        self.generate_variables()

        # Writing node related information:
        for key, value in self.node_info.iteritems():
            new_elt = etree.SubElement(page, 'field', name=key)
            new_elt.text = value

        # Writing generic attributes of the dataset that require no special treatment
        # e.g number of files, path, version
        for key, value in vars(self).iteritems():
            if key not in ('id_dictionary', 'variables', 'global_attributes', 'record', 'netCDFFiles', 'node_info'):
                new_elt = etree.SubElement(page, 'field', name=key)
                new_elt.text = str(value)

        # Writing the identifiers
        for id_key, id_value in self.id_dictionary.iteritems():
            new_elt = etree.SubElement(page, 'field', name=id_key)
            new_elt.text = id_value

        # Getting the global attributes of the different files.
        for global_attr, attr_value in self.global_attributes.iteritems():
            if global_attr != "project_id":
                new_elt = etree.SubElement(page, 'field', name=global_attr)
                new_elt.text = str(attr_value)
            else:
                new_elt = etree.SubElement(page, 'field', name="project")
                new_elt.text = self.global_attributes['project_id']
        # Getting the vars from the different files.
        for var in self.variables:
            new_elt = etree.SubElement(page, 'field', name="variable")
            new_elt.text = var
        # Creating the record for the dataset.
        self.record = doc


class Node(object):
    """
    A node object is used to characterize the node in use in the current user session
    Attributes:
        index_node
        data_node
        certificate_file
        headers
    """
    def __init__(self, data_node, index_node, certificate_file, headers):
        self.data_node = data_node
        self.index_node = index_node
        self.certificate_file = certificate_file
        self.headers = headers


class Session(object):
    """
    This object depicts everything that the user selected and is only valid for this session only.
    Attributes:
        Operation
    """
    def __init__(self, operation, ws_url):
        self.operation = operation
        self.ws_url = ws_url


class NetCDFFile(object):
    """
    NetCDF file object, it contains the main metadata describing this file.
    A NetCDF file necessarily belongs to a dataset.
    For this end, we use single file datasets.
    Attributes:
        path,
        file_name,
        variables,
        global_attributes
    methods:
        extract_file_name(self)
    """
    def __init__(self, path, variables, global_attributes, dataset, node):
        self.type = 'File'
        self.path = path
        self.file_name, self.path = self.extract_file_name()
        # dictionary of variables
        self.record = None
        self.variables = variables
        # dictionary of global attributes
        self.global_attributes = global_attributes
        self.id_dictionary = self.extract_file_ids(dataset, node)

    def extract_file_name(self):
        """
        This function is used to extract the file_name from the path if the path is a file path.
        This is used in the sole case of a dataset containing a single file.
        The directory or file test is performed elsewhere.
        :param self.path: string
        :param self.is_file: Boolean

        :return file_name: String, path: String
        """
        # The directory is generated from the path - filename.
        filename = os.path.basename(self.path)
        file_name = filename
        path = self.path.replace(filename, '')
        return file_name, path

    def extract_file_ids(self, dataset, node):
        """
        extracts different IDs from the file, the dataset and the node.
        :param dataset:
        :param node:

        :return: id_dictionary: dictionary.
        """
        id_dictionary = dict()
        id_dictionary['dataset_id'] = dataset.id_dictionary['id']
        id_dictionary['drs_id'] = dataset.id_dictionary['drs_id']
        id_dictionary['id'] = dataset.id_dictionary['drs_id'] + dot + version_str + dataset.version + dot +self.file_name
        id_dictionary['instance_id'] = dataset.id_dictionary['drs_id'] + dot + version_str + dataset.version + dot +self.file_name
        return id_dictionary

    def generate_record(self, open_netcdf_file, dataset, node):
        page = etree.Element('doc')
        doc = etree.ElementTree(page)
        for key, value in vars(self).iteritems():
            if key not in ('path', 'variables', 'global_attributes', 'dataset', 'node'):
                new_elt = etree.SubElement(page, 'field', name=key)
                new_elt.text = str(value)

        # Writing node information
        for key, value in dataset.node_info.iteritems():
            new_elt = etree.SubElement(page, 'field', name=key)
            new_elt.text = value

        # Writing the different ids
        for id_key, id_value in self.id_dictionary.iteritems():
            new_elt = etree.SubElement(page, 'field', name=id_key)
            new_elt.text = id_value

        # Writing the global attributes
        for global_attr in self.global_attributes:
            # Getting the value of the attributes.
            global_attr_value = getattr(open_netcdf_file, str(global_attr))
            # Updating the dataset global attributes keys and values.
            if global_attr not in dataset.global_attributes:
                dataset.global_attributes[global_attr] = global_attr_value
            if global_attr != "project_id":
                new_elt = etree.SubElement(page, 'field', name=global_attr)
            else:
                new_elt = etree.SubElement(page, 'field', name="project")
            if isinstance(global_attr_value, basestring):
                new_elt.text = global_attr_value
        all_var_names = open_netcdf_file.variables.keys()
        # Writing the variables.
        for var in all_var_names:
            new_elt = etree.SubElement(page, 'field', name="variable")
            new_elt.text = var
        self.record = doc
