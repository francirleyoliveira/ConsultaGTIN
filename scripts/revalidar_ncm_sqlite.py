from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import SQLITE_DB_PATH
from app.validators.gtin import comparar_ncm


def _formatar_timestamps_agora(now: datetime | None = None) -> tuple[str, str]:
    momento = now or datetime.now()
    return (
        momento.strftime("%d/%m/%Y %H:%M:%S"),
        momento.strftime("%Y-%m-%d %H:%M:%S"),
    )


def revalidar_ncms_conn(conn: sqlite3.Connection, now: datetime | None = None) -> tuple[int, int]:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT gtin, ncm_winthor, ncm_oficial, status_sefaz, divergencia_ncm
        FROM consultas_gtin
        WHERE status_sefaz IN ('949', '9490')
        """
    )
    registros = cursor.fetchall()
    total = len(registros)
    atualizados = 0

    for row in registros:
        gtin = row["gtin"]
        ncm_erp = row["ncm_winthor"]
        ncm_sefaz = row["ncm_oficial"]
        divergencia_antiga = row["divergencia_ncm"]
        nova_divergencia = comparar_ncm(ncm_erp, ncm_sefaz)

        if nova_divergencia == divergencia_antiga:
            continue

        agora, agora_ordem = _formatar_timestamps_agora(now)
        cursor.execute(
            """
            UPDATE consultas_gtin
            SET divergencia_ncm = ?, ultima_atualizacao = ?, ultima_atualizacao_ordem = ?
            WHERE gtin = ?
            """,
            (nova_divergencia, agora, agora_ordem, gtin),
        )
        atualizados += 1

    conn.commit()
    return total, atualizados


def revalidar_ncms_sqlite(db_path: Path = SQLITE_DB_PATH) -> tuple[int, int]:
    print(f"Iniciando revalidacao de NCMs no banco: {db_path}")

    if not db_path.exists():
        print("Erro: Banco de dados nao encontrado.")
        return 0, 0

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(db_path)
        total, atualizados = revalidar_ncms_conn(conn)
        print(f"Encontrados {total} registros para processar.")
        print("Processamento concluido.")
        print(f"Total analisado: {total}")
        print(f"Total atualizado: {atualizados}")
        return total, atualizados
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        return 0, 0
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    revalidar_ncms_sqlite()
