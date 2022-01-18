"""SQL scripts execution."""

import os

from databricks import sql
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

SERVER_HOSTNAME = os.environ.get("SERVER_HOSTNAME")
HTTP_PATH = os.environ.get("HTTP_PATH")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

connection = sql.connect(
    server_hostname=SERVER_HOSTNAME, http_path=HTTP_PATH, access_token=ACCESS_TOKEN
)


def execute_sql(file: str) -> None:
    """
    Execute sql file.

    :param file: sql file name
    :return: None
    """
    cursor = connection.cursor()

    with open(file, "r") as sql_file:
        for query in sql_file.read().replace("\n", " ").split(";")[:-1]:
            cursor.execute(query.strip())

    cursor.close()


if __name__ == "__main__":
    execute_sql("database_creation.sql")
    execute_sql("raw_table_creation.sql")
    execute_sql("refined_table_creation.sql")
