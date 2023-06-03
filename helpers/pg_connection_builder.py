from airflow.hooks.base import BaseHook

import psycopg2

class ConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> psycopg2.connect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]
        
        pg = psycopg2.connect(host=conn.host,
                                port=conn.port,
                                dbname=conn.schema,
                                user=conn.login,
                                password=conn.password,
                                sslmode=sslmode)

        return pg