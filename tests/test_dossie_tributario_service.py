from __future__ import annotations

import unittest

from app.config import Settings
from app.services.dossie_tributario_service import DossieTributarioService


class DossieTributarioServiceTest(unittest.TestCase):
    def test_normaliza_catalogo_do_retorno_esperado(self) -> None:
        raw = [
            {
                "CST": "200",
                "DescricaoCST": "Aliquota reduzida",
                "IndIBSCBS": True,
                "classificacoesTributarias": [
                    {
                        "cClassTrib": "200035",
                        "DescricaoClassTrib": "Dentifricios do codigo 3306.10.00 da NCM/SH",
                        "pRedIBS": 60.0,
                        "pRedCBS": 60.0,
                        "TipoAliquota": "Padrao",
                        "IndNFe": True,
                        "IndNFCe": True,
                        "Publicacao": "2026-03-24T00:00:00",
                        "InicioVigencia": "2026-04-01T00:00:00",
                        "Anexo": "VIII",
                        "Link": "https://exemplo.local/base-legal"
                    }
                ]
            }
        ]
        service = DossieTributarioService(Settings(None, None, None, None, None, None), fetcher=lambda: raw)

        catalogo = service.sincronizar_catalogo()
        item_cst = catalogo[0]
        classificacao = item_cst["classificacoes_tributarias"][0]

        self.assertEqual("200", item_cst["cst"])
        self.assertEqual("200035", classificacao["cclasstrib"])
        self.assertEqual("60.0", classificacao["p_red_ibs"])
        self.assertEqual("2026-03-24T00:00:00", classificacao["publicacao"])
        self.assertEqual("2026-04-01T00:00:00", classificacao["inicio_vigencia"])
        self.assertEqual("VIII", classificacao["anexo"])
        self.assertEqual("1", classificacao["ind_nfe"])
        self.assertEqual("https://exemplo.local/base-legal", classificacao["base_legal"])


if __name__ == "__main__":
    unittest.main()
