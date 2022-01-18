"""Install wheel library in all running cluster."""

import argparse

from databricks_library import DatabricksLibrary

parser = argparse.ArgumentParser()
parser.add_argument("--wheel_name", help="wheel name")

args = parser.parse_args()
wheel_name = args.wheel_name


if __name__ == "__main__":
    databricks_library = DatabricksLibrary()
    databricks_library.install_wheel_library(wheel_name)
