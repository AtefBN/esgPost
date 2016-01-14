import subprocess


def index(xml_path, doc, certificate_file, header_form, ws_post_url):
    """
    This method sends a POST request to ESG-Search with generated
    XML descriptors in order to index data to solr.
    This might be easier to be done via pycurl, however for the
    time being we just use a direct curl command.
    :param xml_path: String
    :return: Success or failure message: String
    """
    # In order for pycurl to effectively send the xml data
    # it needs to be quoted to skip special characters.
    """
    data = urllib.quote(doc)
    query = pycurl.Curl()
    query.setopt(query.URL, ws_post_url)
    query.setopt(query.HTTPHEADER, (header_form,))
    query.setopt(query.VERBOSE, True)
    query.setopt(query.INSECURE, True)
    query.setopt(query.KEY, certificate_file)
    query.setopt(query.CERT, certificate_file)
    query.setopt(pycurl.POSTFIELDS, data)
    query.perform()
    query.close()
    """
    curl_query = "curl --insecure --key " + certificate_file + "  --cert " + certificate_file + \
                 " --verbose -X POST -d @" + xml_path + " --header " + header_form + \
                 " " + ws_post_url
    proc = subprocess.Popen([curl_query], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    print "program output:", out
    return out

