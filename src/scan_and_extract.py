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
    mandatory_options['dataset_id'], mandatory_options['master_id'] = extract_dataset_id(path,
                                                                                         file_name='cmip5.output1.BCC.bcc-csm1-1.decadal2000.3hr.atmos.3hr.r1i1p1.v1.'
                                                                                                   'rsdsdiff_3hr_bcc-csm1-1_decadal2000_r1i1p1_200101010130-201512312230.nc')

    for key, value in mandatory_options.iteritems():
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
    of depth and harvests the metadata of the netcdf files found.
    The result is a descriptive XML file.
    :param my_path: String
    :return: modified xml descriptive page
    """
    os.chdir(my_path)
    file_list = os.listdir(my_path)
    for file_name in file_list:
        # in case the directory is a dataset of netcdf files.
        if ".nc" in file_name:
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


def extract_dataset_id(path, file_name):
    dataset_id = ''
    for c in path:
        if c != '/':
            dataset_id += c
        else:
            dataset_id += '.'
    master_id = dataset_id + '|' + file_name
    return dataset_id, master_id
