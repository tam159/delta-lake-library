"""Databricks managed library."""

from typing import Dict, List

from databricks_client import DatabricksClient


class DatabricksLibrary(DatabricksClient):
    """Databricks library client."""

    @property
    def get_installed_wheel_libraries(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get all installed delta lake wheel libraries in all clusters.

        :return: List of installed wheel libraries for each cluster
        """
        clusters_installed_wheel_libraries = {}
        cluster_statuses = self.managed_library.all_cluster_statuses()["statuses"]

        for cluster_status in cluster_statuses:
            cluster_id = cluster_status["cluster_id"]
            installed_wheel_libraries = []

            for library_status in cluster_status["library_statuses"]:
                if (
                    library_status["status"] == "INSTALLED"
                    and "whl" in library_status["library"]
                ):
                    if "delta_lake_library" in library_status["library"]["whl"]:
                        installed_wheel_libraries.append(library_status["library"])

            clusters_installed_wheel_libraries[cluster_id] = installed_wheel_libraries

        return clusters_installed_wheel_libraries

    def uninstall_wheel_libraries(self) -> None:
        """
        Uninstall all delta lake wheel libraries in all clusters.

        :return: None
        """
        for (
            cluster_id,
            installed_wheel_libraries,
        ) in self.get_installed_wheel_libraries.items():
            if len(installed_wheel_libraries) > 0:
                for installed_wheel_library in installed_wheel_libraries:
                    self.managed_library.uninstall_libraries(
                        cluster_id, installed_wheel_library
                    )

    @property
    def get_running_clusters(self) -> List[str]:
        """
        Get running cluster ids.

        :return: List of running cluster ids.
        """
        return [
            cluster["cluster_id"]
            for cluster in self.cluster.list_clusters()["clusters"]
            if cluster["state"] == "RUNNING"
        ]

    def install_wheel_library(self, wheel_name: str) -> None:
        """
        Install wheel library in all running clusters.

        :param wheel_name: wheel name
        :return: None
        """
        wheel_library = {"whl": f"dbfs:/dist/{wheel_name}"}

        for cluster_id in self.get_running_clusters:
            self.managed_library.install_libraries(cluster_id, wheel_library)
