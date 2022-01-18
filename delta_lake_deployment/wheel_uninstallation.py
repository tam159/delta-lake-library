"""Uninstall wheel libraries in all running cluster."""

from databricks_library import DatabricksLibrary

if __name__ == "__main__":
    databricks_library = DatabricksLibrary()
    databricks_library.uninstall_wheel_libraries()
