from distutils.core import setup
"""
setup script to be used to check the environment and deploy to nodes.
"""

setup(name="esgPost",
      version="1.0",
      description="A push mode publisher for ESGF nodes",
      author="AtefBN",
      author_email="abennasser@ipsl.jussieu.fr",
      url="https://github.com/AtefBN/esgPost",
      packages=['src'],
      package_data=['']
      )
