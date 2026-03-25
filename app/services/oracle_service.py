from __future__ import annotations

from pathlib import Path

import oracledb

from app.config import QUERIES_DIR, Settings


_oracle_client_iniciado = False


def inicializar_oracle_client(settings: Settings) -> None:
    global _oracle_client_iniciado

    if _oracle_client_iniciado or not settings.oracle_client_caminho:
        return

    try:
        oracledb.init_oracle_client(lib_dir=settings.oracle_client_caminho)
    except Exception:
        pass
    finally:
        _oracle_client_iniciado = True


def buscar_gtins_winthor(settings: Settings):
    """Busca produtos no banco Oracle do Winthor."""
    inicializar_oracle_client(settings)

    try:
        conexao = oracledb.connect(
            user=settings.db_user,
            password=settings.db_pass,
            dsn=settings.db_dsn,
        )
        cursor = conexao.cursor()
        caminho_sql = Path(QUERIES_DIR) / "consulta_gtins.sql"
        with open(caminho_sql, "r", encoding="utf-8") as arquivo_sql:
            sql = arquivo_sql.read()
        cursor.execute(sql)
        produtos = cursor.fetchall()
        cursor.close()
        conexao.close()
        return produtos
    except Exception as erro:
        print(f"Erro DB: {erro}")
        return []
