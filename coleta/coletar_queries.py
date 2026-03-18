import logging

import pandas as pd

from config.database import conectar_banco

log = logging.getLogger("coletor.queries")

QUERY_COLETA = """
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    ROUND(
        (total_exec_time / NULLIF(SUM(total_exec_time) OVER (), 0) * 100)::numeric,
        2
    ) AS impacto_pct
FROM pg_stat_statements
WHERE calls > 0
ORDER BY total_exec_time DESC
LIMIT 10
"""

def coletar_queries(cliente: dict) -> pd.DataFrame | None:
    """Coleta metriacas do pg_stat_statements do cliente.
       retorna none caso de falha
    """
    nome = cliente["nome"]
    conn = None

    try:
        conn = conectar_banco(cliente)
        df = pd.read_sql_query(QUERY_COLETA, conn)
        log.info(f"[{nome}] {len(df)} queries coletadas")
        return df
    
    except Exception as e:
        log.error(f"[{nome}] Erro ao coletar queries: {e}")
        return None

    finally:
        if conn and not conn.closed:
            conn.close()
