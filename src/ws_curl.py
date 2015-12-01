import subprocess


def index(xml_path, certificate_file, header_form, ws_post_url):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via pycurl, however for the
    time being we just use a direct curl command.
    :param xml_path: String
    :return: Success or failure message: String
    """
    curl_query = "curl --insecure --key " + certificate_file + "  --cert " + certificate_file + \
                 " --verbose -X POST -d @" + xml_path + " --header " + header_form + \
                 " " + ws_post_url
    print("This is the generated curl query " + curl_query)
    proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    print "program output:", out
    return out
