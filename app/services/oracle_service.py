from __future__ import annotations

import logging
from contextlib import closing
from pathlib import Path

import oracledb

from app.config import QUERIES_DIR, Settings


logger = logging.getLogger(__name__)
_oracle_client_iniciado = False
_oracle_client_inicializacao_falhou = False



def inicializar_oracle_client(settings: Settings) -> None:
    global _oracle_client_iniciado, _oracle_client_inicializacao_falhou

    if _oracle_client_iniciado or _oracle_client_inicializacao_falhou or not settings.oracle_client_caminho:
        return

    try:
        oracledb.init_oracle_client(lib_dir=settings.oracle_client_caminho)
    except Exception:
        _oracle_client_inicializacao_falhou = True
        logger.warning(
            "Falha ao inicializar Oracle Client; novas tentativas automaticas ficaram desabilitadas ate reiniciar a aplicacao.",
            exc_info=True,
        )
        return

    _oracle_client_iniciado = True



def _carregar_sql_consulta() -> str:
    caminho_sql = Path(QUERIES_DIR) / "consulta_gtins.sql"
    return caminho_sql.read_text(encoding="utf-8")



def buscar_gtins_winthor(settings: Settings):
    """Busca produtos no banco Oracle do Winthor."""
    inicializar_oracle_client(settings)

    try:
        with closing(
            oracledb.connect(
                user=settings.db_user,
                password=settings.db_pass,
                dsn=settings.db_dsn,
            )
        ) as conexao, closing(conexao.cursor()) as cursor:
            cursor.execute(_carregar_sql_consulta())
            return cursor.fetchall()
    except Exception:
        logger.exception("Falha ao consultar GTINs no Oracle Winthor.")
        return []
