from datetime import datetime
from config.database import conectar_monitoramento

def salvar_metricas(cliente, df):

    conn = conectar_monitoramento()
    cur = conn.cursor()

    data_coleta = datetime.now()

    for _, row in df.iterrows():

        query_text = row["query"]

        #verificar se a quary ja existe
        cur.execute(
            "SELECT id FROM queries WHERE query = %s",
            (query_text,)
        )

        result = cur.fetchone()

        if result:
            query_id = result[0]
        else:
            cur.execute(
                "INSERT INTO queries (query) VALUES (%s) RETURNING id",
                (query_text,)
            )

            query_id = cur.fetchone()[0]

        calls = row["calls"]
        total_exec_time = row["total_exec_time"]
        mean_exec_time = row["mean_exec_time"]

        impacto = calls * total_exec_time

        cur.execute(
            """
            INSERT INTO metricas_queries 
            (query_id, cliente, calls, total_exec_time, mean_exec_time, impacto, data_coleta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                query_id,
                cliente,
                calls,
                total_exec_time,
                mean_exec_time,
                impacto,
                data_coleta
            )
        )

    conn.commit()
    cur.close()