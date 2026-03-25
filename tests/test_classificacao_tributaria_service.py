from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from app.config import Settings
from app.services.anexo_tributario_service import AnexoTributarioService
from app.services.classificacao_tributaria_service import ClassificacaoTributariaService
from app.services.conformidade_scraper_service import ConformidadeScraperService
from app.services.dossie_tributario_service import DossieTributarioService
from app.services.sqlite_service import ConsultaGtinRepository


class ClassificacaoTributariaServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path("tests") / ".tmp_classificacao_service" / uuid.uuid4().hex
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.repo = ConsultaGtinRepository(self.temp_dir / "test.db")
        self.settings = Settings(
            db_user=None,
            db_pass=None,
            db_dsn=None,
            cert_senha=None,
            cert_caminho=None,
            oracle_client_caminho=None,
        )

    def test_processa_pipeline_completa_e_persiste_cenarios_por_ncm(self) -> None:
        chamadas_catalogo = [0]
        chamadas_anexos = [0]

        def sefaz_fake(gtin: str, _settings: Settings) -> dict[str, str]:
            return {
                "status": "949",
                "motivo": "OK",
                "xProd": "Cerveja Teste",
                "NCM": "22030000",
                "CEST": "0300100",
                "data_hora": "24/03/2026 11:00:00",
            }

        scraper = ConformidadeScraperService(
            self.settings,
            extractor=lambda ncm: [
                {
                    "ncm": ncm,
                    "cst": "060",
                    "cclasstrib": "CLASS001",
                    "condicao_legal": "Consumo humano",
                }
            ],
        )
        dossie = DossieTributarioService(
            self.settings,
            fetcher=lambda: chamadas_catalogo.__setitem__(0, chamadas_catalogo[0] + 1) or [
                {
                    "CST": "060",
                    "DescricaoCST": "Teste",
                    "IndIBSCBS": True,
                    "classificacoesTributarias": [
                        {
                            "cClassTrib": "CLASS001",
                            "DescricaoClassTrib": "Tributacao integral",
                            "Anexo": "IV",
                            "IndNFe": True,
                            "IndNFCe": True,
                            "Link": "https://exemplo.local/base-legal",
                        }
                    ],
                }
            ],
        )
        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            sefaz_consultor=sefaz_fake,
            scraper_service=scraper,
            dossie_service=dossie,
            anexo_service=AnexoTributarioService(
                self.settings,
                fetcher=lambda: chamadas_anexos.__setitem__(0, chamadas_anexos[0] + 1) or [
                    {
                        "Anexo": "IV",
                        "DescricaoAnexo": "Anexo IV",
                        "Especificidades": [
                            {
                                "CodigoEspecificidade": "ESP1",
                                "DescricaoEspecificidade": "Faixa teste",
                                "Valor": "1",
                            }
                        ],
                    }
                ],
            ),
        )

        resultado = service.processar_produto(("100", "7891234567895", "22030000"))

        self.assertEqual("949", resultado["consulta"]["status_sefaz"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertEqual(1, chamadas_catalogo[0])
        self.assertEqual(1, chamadas_anexos[0])
        cenarios = self.repo.listar_cenarios_tributarios()
        self.assertEqual(1, len(cenarios))
        self.assertEqual("22030000", cenarios[0]["ncm"])
        self.assertEqual("CLASS001", self.repo.obter_dossie_cache("CLASS001")["cclasstrib"])
        self.assertEqual("IV", cenarios[0]["anexo"])
        self.assertEqual("IV", self.repo.obter_anexo_cache("IV")["anexo"])

        service.processar_produto(("100", "7891234567895", "22030000"))
        self.assertEqual(1, chamadas_catalogo[0])
        self.assertEqual(1, chamadas_anexos[0])

    def test_processa_ncm_diretamente_sem_gtin(self) -> None:
        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            scraper_service=ConformidadeScraperService(
                self.settings,
                extractor=lambda ncm: [
                    {
                        "ncm": ncm,
                        "cst": "200",
                        "cclasstrib": "200035",
                        "condicao_legal": "Baixa renda",
                    }
                ],
            ),
            dossie_service=DossieTributarioService(
                self.settings,
                fetcher=lambda: [
                    {
                        "CST": "200",
                        "DescricaoCST": "Aliquota reduzida",
                        "classificacoesTributarias": [
                            {
                                "cClassTrib": "200035",
                                "DescricaoClassTrib": "Dentifricios",
                                "IndNFe": True,
                                "IndNFCe": True,
                                "Link": "https://exemplo.local/base-legal",
                            }
                        ],
                    }
                ],
            ),
        )

        resultado = service.processar_ncm("33061000")

        self.assertEqual("33061000", resultado["ncm"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertEqual("200035", self.repo.listar_cenarios_tributarios()[0]["cclasstrib"])

    def test_classifica_por_ncm_do_erp_mesmo_com_gtin_fora_do_prefixo(self) -> None:
        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            sefaz_consultor=lambda *_args: self.fail("Nao deveria consultar a Sefaz"),
            scraper_service=ConformidadeScraperService(
                self.settings,
                extractor=lambda ncm: [
                    {
                        "ncm": ncm,
                        "cst": "000",
                        "cclasstrib": "000001",
                        "condicao_legal": "Tributacao integral",
                    }
                ],
            ),
            dossie_service=DossieTributarioService(
                self.settings,
                fetcher=lambda: [
                    {
                        "CST": "000",
                        "DescricaoCST": "Integral",
                        "classificacoesTributarias": [
                            {
                                "cClassTrib": "000001",
                                "DescricaoClassTrib": "Tributacao integral",
                                "IndNFe": True,
                                "IndNFCe": True,
                                "Link": "https://exemplo.local/base-legal",
                            }
                        ],
                    }
                ],
            ),
        )

        resultado = service.processar_produto(("200", "12345670", "18063220"))

        self.assertEqual("GTIN_FORA_GS1_BR", resultado["consulta"]["status_sefaz"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertEqual("18063220", self.repo.listar_cenarios_tributarios()[0]["ncm"])


if __name__ == "__main__":
    unittest.main()
