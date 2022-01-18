"""Databricks client."""

import os
from typing import Optional

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk.service import (
    ClusterService,
    DbfsService,
    DeltaPipelinesService,
    GroupsService,
    InstancePoolService,
    JobsService,
    ManagedLibraryService,
    PolicyService,
    ReposService,
    SecretService,
    TokenService,
    WorkspaceService,
)
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")


class DatabricksClient:
    """
    Databricks client.

    :param host: Databricks host
    :param token: Databricks token
    """

    def __init__(
        self,
        host: Optional[str] = DATABRICKS_HOST,
        token: Optional[str] = DATABRICKS_TOKEN,
    ):
        self.api_client = ApiClient(host=host, token=token)
        self.jobs = JobsService(self.api_client)
        self.cluster = ClusterService(self.api_client)
        self.policy = PolicyService(self.api_client)
        self.managed_library = ManagedLibraryService(self.api_client)
        self.dbfs = DbfsService(self.api_client)
        self.workspace = WorkspaceService(self.api_client)
        self.secret = SecretService(self.api_client)
        self.groups = GroupsService(self.api_client)
        self.token = TokenService(self.api_client)
        self.instance_pool = InstancePoolService(self.api_client)
        self.delta_pipelines = DeltaPipelinesService(self.api_client)
        self.repo = ReposService(self.api_client)
