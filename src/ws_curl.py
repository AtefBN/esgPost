import subprocess
import os
from utils import *


def index(output_path, certificate_file, header_form, ws_post_url):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via pycurl, however for the
    time being we just use a direct curl command.
    :param output_path: String
    :return: Success or failure message: String
    """
    # In order for pycurl to effectively send the xml data
    # it needs to be quoted to skip special characters.
    os.chdir(output_path)
    file_list = os.listdir(output_path)
    for record in file_list:
        if output_path.endswith(slash):
            record_path = output_path + record
        else:
            record_path = output_path + slash + record
        curl_query = "curl --key " + certificate_file + "  --cert " + certificate_file + \
                     " --verbose -X POST -d @" + record_path + " --header " + header_form + \
                     " " + ws_post_url
        proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        print "program output:", out
