#from distutils.core import setup
import imp
"""
setup script to be used to check the environment and deploy to nodes.
"""


def check_versions():
    """
    Checks that the machine on which installation occurs has the required libs and versions
    :return: success or failure message
    """
    try:
        imp.find_module('lxml')
        imp.find_module('netCDF4')
        from lxml import etree
        import netCDF4
        print(etree.LXML_VERSION)
        print(netCDF4.__version__)

    except:
        print("Required library not found on machine, please refer to the install manual.")




"""
setup(name="esgPost",
      version="0.1",
      description="A push mode publisher for ESGF nodes",
      author="AtefBN",
      author_email="abennasser@ipsl.jussieu.fr",
      url="https://github.com/AtefBN/esgPost",
      packages=['src'],
      package_data=['']
      )
"""

check_versions()
