"""
This script is designed to unpublish datasets
that were previously published using the push tool.
curl --insecure --key ~/.esg/credentials.pem --cert ~/.esg/credentials.pem --verbose -X POST -d @dataset.xml --header
"Content-Type:application/xml" https://test-datanode.jpl.nasa.gov/esg-search/ws/unpublish
"""
import sys


def main():
    argv = sys.argv[1:]

    return None


if __name__ == "__main__":
    main()
