import logging
from datetime import datetime

import pandas as pd 

from config.database import conectar_monitoramento

log = logging.getLogger("coletor.persistencia")


def salvar_metricas(cliente: str, df: pd.DataFrame) -> bool:
    """insere as metricas no banco de monitoramento
       retorna true em caso de sucesso, false em caso de falha
    """
    if df is None or df.empty:
        log.warning(f"[{cliente}] DataFrame vazio, nada a salvar.")
        return False
    
    conn = None

    try:
        conn = conectar_monitoramento()
        cur = conn.cursor()
        data_coleta = datetime.now()

        for _, row in df.iterrows():
            query_text = row["query"]

            cur.execute("SELECT id FROM queries WHERE query = %s", (query_text,))
            result = cur.fetchone()
 
            if result:
                query_id = result[0]
            else:
                cur.execute(
                    "INSERT INTO queries (query) VALUES (%s) RETURNING id",
                    (query_text,),
                )
                query_id = cur.fetchone()[0]
 
            calls = row["calls"]
            total_exec_time = row["total_exec_time"]
            mean_exec_time = row["mean_exec_time"]
            impacto_pct = row.get("impacto_pct", 0.0) or 0.0
 
            cur.execute(
                """
                INSERT INTO metricas_queries
                    (query_id, cliente, calls, total_exec_time,
                     mean_exec_time, impacto_pct, data_coleta)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    query_id,
                    cliente,
                    int(calls),
                    float(total_exec_time),
                    float(mean_exec_time),
                    float(impacto_pct),
                    data_coleta,
                ),
            )
 
        conn.commit()
        log.info(f"[{cliente}] {len(df)} métricas salvas")
        return True
 
    except Exception as e:
        log.error(f"[{cliente}] Erro ao salvar métricas: {e}")
        if conn:
            conn.rollback()
        return False
 
    finally:
        if conn and not conn.closed:
            cur.close()
            conn.close()