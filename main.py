import logging
import os
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.clientes import clientes
from coleta.coletar_queries import coletar_queries
from persistencia.salvar_metricas import salvar_metricas

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("coletor.main")
 
# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
INTERVALO_MINUTOS = int(os.environ.get("INTERVALO_COLETA_MIN", "15"))
 
 
# ---------------------------------------------------------------------------
# Job de coleta
# ---------------------------------------------------------------------------
def executar_coleta() -> None:
    """
    Processa todos os clientes de forma isolada:
    a falha de um cliente não interrompe os demais.
    """
    inicio = datetime.now(timezone.utc)
    log.info(f"=== Início do ciclo [{inicio.strftime('%H:%M:%S UTC')}] ===")
 
    sucesso = 0
    falha = 0
 
    for cliente in clientes:
        nome = cliente["nome"]
        log.info(f"Coletando dados de: {nome}")
 
        try:
            df = coletar_queries(cliente)
 
            if df is None:
                log.warning(f"[{nome}] Coleta retornou vazio, pulando persistência.")
                falha += 1
                continue
 
            ok = salvar_metricas(nome, df)
            if ok:
                sucesso += 1
            else:
                falha += 1
 
        except Exception as e:
            # Captura qualquer exceção inesperada para não derrubar o scheduler
            log.error(f"[{nome}] Erro inesperado: {e}", exc_info=True)
            falha += 1
 
    duracao = (datetime.now(timezone.utc) - inicio).total_seconds()
    log.info(
        f"=== Ciclo concluído em {duracao:.1f}s — "
        f"{sucesso} ok / {falha} falha(s) ==="
    )
 
 
# ---------------------------------------------------------------------------
# Entrada principal
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Iniciando coletor de métricas PostgreSQL")
    log.info(f"Clientes: {[c['nome'] for c in clientes]}")
    log.info(f"Intervalo: {INTERVALO_MINUTOS} minuto(s)")
 
    # Coleta imediata ao iniciar (não espera o primeiro intervalo)
    executar_coleta()
 
    # Agendamento das coletas subsequentes
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        func=executar_coleta,
        trigger=IntervalTrigger(minutes=INTERVALO_MINUTOS),
        id="coleta_metricas",
        name="Coleta pg_stat_statements",
        misfire_grace_time=60,  # Tolera até 60s de atraso
        coalesce=True,          # Se atrasou N vezes, roda apenas uma
        max_instances=1,        # Nunca duas coletas simultâneas
    )
 
    log.info(f"Scheduler ativo. Próxima coleta em {INTERVALO_MINUTOS} min.")
    log.info("Pressione Ctrl+C para encerrar.")
 
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Coletor encerrado.")
 
 
if __name__ == "__main__":
    main()