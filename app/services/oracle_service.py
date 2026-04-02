from __future__ import annotations

import logging
from contextlib import closing
from pathlib import Path

import oracledb

from app.config import QUERIES_DIR, Settings


logger = logging.getLogger(__name__)
_oracle_client_iniciado = False



def inicializar_oracle_client(settings: Settings) -> None:
    global _oracle_client_iniciado

    if _oracle_client_iniciado or not settings.oracle_client_caminho:
        return

    try:
        oracledb.init_oracle_client(lib_dir=settings.oracle_client_caminho)
    except Exception:
        logger.warning(
            "Falha ao inicializar Oracle Client; uma nova tentativa sera feita na proxima consulta.",
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
