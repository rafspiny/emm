import logging
import os

log = logging.getLogger(__name__)


def validate_sql_folder(sql_folder_path: str) -> bool:
    """
    Make sure that the folder exists and contains the right files
    """
    if sql_folder_path:
        path: str = os.path.join("sql", "projects", sql_folder_path)
        log.debug(f"Checking {sql_folder_path} folder existence in {os.getcwd()}")
        if os.path.isdir(path):
            # Folder exists. Let's check if it has the right structure.
            schema_file = os.path.join(path, "schema.sql")
            data_file = os.path.join(path, "data.sql")
            log.debug(f"Found {path} folder")

            return os.path.isfile(schema_file) and os.path.isfile(data_file)

    return False
