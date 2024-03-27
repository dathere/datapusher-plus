import os
import uuid

from collections.abc import MutableMapping
from typing import get_type_hints, Union
# from dotenv import load_dotenv


env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
# load_dotenv(env_file)

_DATABASE_URI = "postgresql://ckan_default:pass@localhost/ckan_default"
_WRITE_ENGINE_URL = "postgresql://datapusher:YOURPASSWORD@localhost/datastore_default"
_TYPES = "String", "Float", "Integer", "DateTime", "Date", "NULL"
_TYPE_MAPPING = {
    "String": "text",
    "Integer": "numeric",
    "Float": "numeric",
    "DateTime": "timestamp",
    "Date": "timestamp",
    "NULL": "text",
}


class DataPusherPlusError(Exception):
    pass


def _parse_bool(val: Union[str, bool]) -> bool:  # pylint: disable=E1136
    return val if type(val) == bool else val.lower() in ["true", "yes", "1"]


# DataPusherPlusConfig class with required fields, default values, type checking, and typecasting for int and bool values
class DataPusherPlusConfig(MutableMapping):
    # ckan_service_provider settings
    SQLALCHEMY_DATABASE_URI: str = _DATABASE_URI
    WRITE_ENGINE_URL: str = _WRITE_ENGINE_URL
    DEBUG: bool = False
    TESTING: bool = False
    SECRET_KEY: str = str(uuid.uuid4())
    USERNAME: str = str(uuid.uuid4())
    PASSWORD: str = str(uuid.uuid4())
    NAME: str = "datapusher"
    HOST: str = "0.0.0.0"
    PORT: int = 8800
    LOG_FILE: str = "/tmp/ckan_service.log"
    STDERR: bool = True
    KEEP_JOBS_AGE: int = 60

    MAX_CONTENT_LENGTH: str = "125600000"
    IGNORE_FILE_HASH: bool = True
    CHUNK_SIZE: str = "16384"
    DOWNLOAD_TIMEOUT: int = 300
    SSL_VERIFY: bool = False
    DOWNLOAD_PROXY: str = ""

    TYPES: tuple = _TYPES
    TYPE_MAPPING: dict = _TYPE_MAPPING

    PII_SCREENING: bool = False
    PII_QUICK_SCREEN: bool = False
    PII_FOUND_ABORT: bool = False
    PII_SHOW_CANDIDATES: bool = False
    PII_REGEX_RESOURCE_ID_OR_ALIAS: str = ""

    QSV_BIN: str = "/usr/local/bin/qsvdp"
    FILE_BIN: str = "/usr/bin/file"

    PREFER_DMY: bool = False
    PREVIEW_ROWS: int = 0
    
    AUTO_INDEX_THRESHOLD: int = 3
    AUTO_UNIQUE_INDEX:bool = True
    AUTO_INDEX_DATES: bool = True
    
    SORT_AND_DUPE_CHECK: bool = True
    DEDUP: bool = False
    DEFAULT_EXCEL_SHEET: int = 0
    
    ADD_SUMMARY_STATS_RESOURCE: bool = False
    SUMMARY_STATS_OPTIONS: str = ""
    
    AUTO_ALIAS: bool = True
    AUTO_ALIAS_UNIQUE: bool = False

    """
    Map environment variables to class fields according to these rules:
      - Field won't be parsed unless it has a type annotation
      - Field will be skipped if not in all caps
      - Class field and environment variable name are the same
    """

    def __init__(self, env):
        for field in self.__annotations__:
            if not field.isupper():
                continue

            # Raise DataPusherPlusError if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and env.get(field) is None:
                raise DataPusherPlusError("The {} field is required".format(field))

            # Cast env var value to expected type and raise DataPusherPlusError on failure
            try:
                var_type = get_type_hints(DataPusherPlusConfig)[field]
                if var_type == bool:
                    value = _parse_bool(env.get(field, default_value))
                else:
                    value = var_type(env.get(field, default_value))

                self.__setattr__(field, value)
            except ValueError:
                raise DataPusherPlusError(
                    'Unable to cast value of "{}" to type "{}" for "{}" field'.format(
                        env[field], var_type, field
                    )
                )

    def __repr__(self):
        return str(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def copy(self):
        return self.__dict__.copy()

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]


# Expose config object for app to import
config = DataPusherPlusConfig(os.environ)
# Expose these variables so ckanserviceprovider can use it
SQLALCHEMY_DATABASE_URI = config.get("SQLALCHEMY_DATABASE_URI")
WRITE_ENGINE_URL = config.get("WRITE_ENGINE_URL")
DEBUG = config.get("DEBUG")
TESTING = config.get("TESTING")
SECRET_KEY = config.get("SECRET_KEY")
USERNAME = config.get("USERNAME")
PASSWORD = config.get("PASSWORD")
NAME = config.get("NAME")
HOST = config.get("HOST")
PORT = config.get("PORT")
LOG_FILE = config.get("LOG_FILE")
STDERR = config.get("STDERR")
KEEP_JOBS_AGE = config.get("KEEP_JOBS_AGE")
