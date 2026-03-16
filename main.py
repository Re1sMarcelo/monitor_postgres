from config.clientes import clientes
from coleta.coletar_queries import coletar_queries
from persistencia.salvar_metricas import salvar_metricas

def main():

    for cliente in clientes:
        print("coletando dados de:", cliente["nome"])

        df_metricas = coletar_queries(cliente)

        salvar_metricas(cliente["nome"], df_metricas)


if __name__ == "__main__":
    main()