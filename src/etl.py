import duckdb

from .config_duckdb import duckdb_connection


@duckdb_connection
def load_table_to_stg(
    conn: duckdb.DuckDBPyConnection,
    path_to_file: str,
    table_name: str,
    drop_if_exists: bool,
    create_if_not_exists: bool,
) -> None:
    if drop_if_exists:
        conn.sql(f"DROP TABLE IF EXISTS staging.{table_name};")

    if create_if_not_exists:
        conn.sql(f"""
        CREATE TABLE IF NOT EXISTS staging.{table_name} AS
        SELECT *
        FROM ST_READ('{path_to_file}');
        """)


@duckdb_connection
def transform_to_intermediate(
    conn: duckdb.DuckDBPyConnection,
    tables: list
) -> None:
    queries = []
    for table in tables:
        query = f"""
        SELECT
            number,
            street,
            unit,
            city,
            district,
            region,
            postcode,
            ST_Y(geom) AS latitude,
            ST_X(geom) AS longitude 
        FROM
            staging.{table}
        """
        queries.append(query)

    queries_union = " \nUNION ALL\n".join(queries)
    query_final = "CREATE OR REPLACE VIEW intermediate.int_brazil_addresses AS\n" + queries_union
    conn.sql(query_final)


@duckdb_connection
def transform_to_mart(conn: duckdb.DuckDBPyConnection):
    query = """
    CREATE OR REPLACE TABLE mart.tb_dm_geoloc_ceps AS
    SELECT
        postcode AS CEP,
        city AS CIDADE,
        region AS REGIAO,
        MEDIAN(latitude) AS LATITUDE,
        MEDIAN(longitude) AS LONGITUDE
    FROM
        intermediate.int_brazil_addresses
    GROUP BY
        ALL
    """

    conn.sql(query)