import sqlite3
from pathlib import Path
from datetime import datetime
import sys
import os

# Adiciona o diretorio raiz ao sys.path para importar os modulos do app
sys.path.append(os.getcwd())

from app.config import SQLITE_DB_PATH
from app.validators.gtin import comparar_ncm

def revalidar_ncms_sqlite():
    print(f"Iniciando revalidacao de NCMs no banco: {SQLITE_DB_PATH}")
    
    if not SQLITE_DB_PATH.exists():
        print("Erro: Banco de dados nao encontrado.")
        return

    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Seleciona apenas registros que tiveram retorno da SEFAZ
        cursor.execute("""
            SELECT gtin, ncm_winthor, ncm_oficial, status_sefaz, divergencia_ncm 
            FROM consultas_gtin 
            WHERE status_sefaz IN ('949', '9490')
        """)
        
        registros = cursor.fetchall()
        total = len(registros)
        print(f"Encontrados {total} registros para processar.")

        atualizados = 0
        for row in registros:
            gtin = row['gtin']
            ncm_erp = row['ncm_winthor']
            ncm_sefaz = row['ncm_oficial']
            divergencia_antiga = row['divergencia_ncm']

            # Calcula nova divergencia com a logica aprimorada
            nova_divergencia = comparar_ncm(ncm_erp, ncm_sefaz)

            if nova_divergencia != divergencia_antiga:
                agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                cursor.execute("""
                    UPDATE consultas_gtin 
                    SET divergencia_ncm = ?, ultima_atualizacao = ?
                    WHERE gtin = ?
                """, (nova_divergencia, agora, gtin))
                atualizados += 1

        conn.commit()
        print(f"Processamento concluido.")
        print(f"Total analisado: {total}")
        print(f"Total atualizado: {atualizados}")

    except Exception as e:
        print(f"Erro durante o processamento: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    revalidar_ncms_sqlite()
