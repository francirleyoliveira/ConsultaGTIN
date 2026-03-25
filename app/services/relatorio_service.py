from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from app.config import RELATORIOS_DIR, ensure_output_dirs


def exportar_consultas_excel(dados_relatorio: list[dict], prefixo: str = "Relatorio_Validade_GTIN") -> Path:
    ensure_output_dirs()
    df = pd.DataFrame(dados_relatorio)
    nome_arquivo = RELATORIOS_DIR / f"{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(nome_arquivo, index=False)
    return nome_arquivo
