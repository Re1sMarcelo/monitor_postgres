import pandas as pd
from config.database import conectar_banco

def coletar_queries(cliente):

    conn = conectar_banco(cliente)

    query = """
     SELECT
        query,
        calls,
        total_exec_time,
        mean_exec_time
    FROM pg_stat_statements
    ORDER BY total_exec_time DESC
    LIMIT 10
    """

    df = pd.read_sql_query(query, conn)

    conn.close()

    return df