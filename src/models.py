import os
slash = '/'
dot = '.'
pipe = '|'


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

    def __init__(self, path, file_type, version, is_file, number_of_files, netcdf_files, variables, node):
        self.path = path
        self.version = version
        self.is_file = is_file
        if self.is_file:
            self.file_name, self.path = self.extract_file_name()
        self.file_type = file_type
        self.number_of_files = number_of_files
        self.netCDFFiles = netcdf_files
        self.variables = variables
        # Initializing Ids
        self.drs, self.master_id, self.instance_id, self.id = self.extract_ids(node)

    def extract_file_name(self):
        """
        This function is used to extract the file_name from the path if the path is a file path.
        This is used in the sole case of a dataset containing a single file.
        The directory or file test is performed elsewhere.
        :param self.path: string
        :param self.is_file: Boolean
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
                print(c)
                continue
            # building the base which coincides with the DRS id.
            elif c != slash:
                base_id += c
            # replacing the inner slashes with dots.
            else:
                base_id += dot
        # Building the master and dataset id out of the base id.
        master_id = base_id
        id = base_id + self.version + pipe + node.data_node
        instance_id = base_id + self.version
        drs_id = base_id
        return drs_id, master_id, instance_id, id


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
    def __init__(self, operation):
        self.operation = operation


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
        self.path = path
        self.file_name, self.path = self.extract_file_name(self)
        # dictionary of variables
        self.variables = variables
        # dictionary of global attributes
        self.global_attributes = global_attributes
        self.dataset_id, self.drs_id, self.id, self.instance_id = self.extract_file_ids(self, dataset, node)

    def extract_file_name(self):
        """
        This function is used to extract the file_name from the path if the path is a file path.
        This is used in the sole case of a dataset containing a single file.
        The directory or file test is performed elsewhere.
        :param self.path: string
        :param self.is_file: Boolean
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
        :return: the different IDs
        """
        dataset_id = dataset.id
        drs_id = dataset.drs_id
        id = drs_id + dot + dataset.version + self.file_name
        instance_id = drs_id + dataset.version + self.file_name
        return dataset_id, drs_id, id, instance_id

