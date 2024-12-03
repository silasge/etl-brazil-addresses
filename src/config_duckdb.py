from functools import wraps
from pathlib import Path
from typing import Callable

import duckdb

from .utils import get_configs


def duckdb_connection(func: Callable = None) -> Callable:
    @wraps(func)
    def decorator(conn=None, *args, **kwargs):
        DB_CONFIG = get_configs("./config/db.toml")

        # Creating database if not exists
        dir_ = Path(DB_CONFIG["db"]["dir"])
        name = Path(DB_CONFIG["db"]["name"])
        dir_.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(dir_ / name)

        # Creating schemas if not exists
        schemas = DB_CONFIG["db"]["schemas"]
        list(map(lambda s: conn.sql(f"CREATE SCHEMA IF NOT EXISTS {s};"), schemas))

        # Installing and loadling extensions if not installed and loaded
        extensions = DB_CONFIG["db"]["extensions"]
        list(map(lambda e: conn.sql(f"INSTALL {e}; LOAD {e};"), extensions))

        # Do stuff in the databse
        func(conn=conn, *args, **kwargs)

        # Close connection
        conn.close()
    
    return decorator
