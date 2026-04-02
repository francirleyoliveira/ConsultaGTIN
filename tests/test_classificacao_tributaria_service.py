from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from app.config import Settings
from app.services.anexo_tributario_service import AnexoTributarioService
from app.services.classificacao_tributaria_service import ClassificacaoTributariaService
from app.services.conformidade_scraper_service import ConformidadeScraperService, ModuloBIndisponivelError
from app.services.dossie_tributario_service import DossieTributarioService
from app.services.gtin_health_service import GtinServiceHealthMonitor
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

    def _monitor_ok(self) -> GtinServiceHealthMonitor:
        return GtinServiceHealthMonitor(
            self.settings,
            checker=lambda *_args, **_kwargs: {
                "ok": True,
                "status": "9498",
                "motivo": "Servico ativo",
                "gtin_teste": "7891032015604",
            },
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
            gtin_health_monitor=self._monitor_ok(),
        )

        resultado = service.processar_produto(("100", "7891234567895", "22030000", "Cerveja ERP"))

        self.assertEqual("949", resultado["consulta"]["status_sefaz"])
        self.assertEqual("Cerveja ERP", resultado["consulta"]["descricao_erp"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertEqual(1, chamadas_catalogo[0])
        self.assertEqual(1, chamadas_anexos[0])
        cenarios = self.repo.listar_cenarios_tributarios()
        self.assertEqual(1, len(cenarios))
        self.assertEqual("22030000", cenarios[0]["ncm"])
        self.assertEqual("CLASS001", self.repo.obter_dossie_cache("CLASS001")["cclasstrib"])
        self.assertEqual("IV", cenarios[0]["anexo"])
        self.assertEqual("IV", self.repo.obter_anexo_cache("IV")["anexo"])

        service.processar_produto(("100", "7891234567895", "22030000", "Cerveja ERP"))
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
            gtin_health_monitor=self._monitor_ok(),
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
            gtin_health_monitor=self._monitor_ok(),
        )

        resultado = service.processar_produto(("200", "12345670", "18063220"))

        self.assertEqual("GTIN_FORA_GS1_BR", resultado["consulta"]["status_sefaz"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertEqual("18063220", self.repo.listar_cenarios_tributarios()[0]["ncm"])

    def test_retorna_sefaz_indisponivel_quando_preflight_falha(self) -> None:
        chamadas_sefaz = [0]
        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            sefaz_consultor=lambda *_args: chamadas_sefaz.__setitem__(0, chamadas_sefaz[0] + 1) or {},
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
            gtin_health_monitor=GtinServiceHealthMonitor(
                self.settings,
                checker=lambda *_args, **_kwargs: {
                    "ok": False,
                    "status": "Erro",
                    "motivo": "HTTP 404",
                    "gtin_teste": "7891032015604",
                },
            ),
        )

        resultado = service.processar_produto(("300", "7891234567895", "18063220"))

        self.assertEqual("SEFAZ_INDISPONIVEL", resultado["consulta"]["status_sefaz"])
        self.assertEqual("HTTP 404", resultado["consulta"]["motivo_sefaz"])
        self.assertEqual(0, chamadas_sefaz[0])
        self.assertEqual(1, len(resultado["cenarios"]))

    def test_recalcula_divergencia_com_cache_oficial_quando_erp_e_corrigido(self) -> None:
        self.repo.upsert_consulta(
            {
                "cod_winthor": "400",
                "gtin": "7891234567895",
                "data_hora_resposta": "24/03/2026 10:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "Consulta realizada com sucesso",
                "ncm_winthor": "22041000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "DIVERGENTE: ERP(22041000) != GS1(22030000)",
                "descricao_produto": "Produto com cache",
                "descricao_erp": "Produto ERP em cache",
                "cest": "1234567",
            }
        )

        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            sefaz_consultor=lambda *_args: self.fail("Nao deveria consultar a Sefaz com preflight indisponivel"),
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
            gtin_health_monitor=GtinServiceHealthMonitor(
                self.settings,
                checker=lambda *_args, **_kwargs: {
                    "ok": False,
                    "status": "Erro",
                    "motivo": "HTTP 404",
                    "gtin_teste": "7891032015604",
                },
            ),
        )

        resultado = service.processar_produto(("400", "7891234567895", "22030000"))
        consulta = resultado["consulta"]

        self.assertEqual("SEFAZ_INDISPONIVEL", consulta["status_sefaz"])
        self.assertEqual("22030000", consulta["ncm_winthor"])
        self.assertEqual("22030000", consulta["ncm_oficial"])
        self.assertEqual("OK", consulta["divergencia_ncm"])
        self.assertIn("Usando cache oficial GS1", consulta["motivo_sefaz"])
        self.assertEqual("Produto com cache", consulta["descricao_produto"])
        self.assertEqual("Produto ERP em cache", consulta["descricao_erp"])
        self.assertEqual("1234567", consulta["cest"])

        persistida = self.repo.obter_consulta_por_gtin("7891234567895")
        self.assertIsNotNone(persistida)
        self.assertEqual("22030000", persistida["ncm_oficial"])
        self.assertEqual("OK", persistida["divergencia_ncm"])



    def test_preserva_cenarios_em_cache_quando_modulo_b_indisponivel(self) -> None:
        self.repo.salvar_cenarios_tributarios(
            "33061000",
            [
                {
                    "ncm": "33061000",
                    "cst": "200",
                    "cclasstrib": "200035",
                    "condicao_legal": "Cache local",
                    "descricao_dossie": "Cenario em cache",
                    "fonte": "cache",
                }
            ],
        )

        service = ClassificacaoTributariaService(
            self.settings,
            self.repo,
            scraper_service=ConformidadeScraperService(
                self.settings,
                extractor=lambda _ncm: (_ for _ in ()).throw(ModuloBIndisponivelError("Falha temporaria no portal")),
            ),
            gtin_health_monitor=self._monitor_ok(),
        )

        resultado = service.processar_ncm("33061000")

        self.assertEqual("cache", resultado["origem_cenarios"])
        self.assertEqual(1, len(resultado["cenarios"]))
        self.assertIn("Cenarios existentes no cache local foram preservados.", resultado["warnings"])
        cenarios = self.repo.listar_cenarios_tributarios({"ncm": "33061000"})
        self.assertEqual(1, len(cenarios))
        self.assertEqual("200035", cenarios[0]["cclasstrib"])


if __name__ == "__main__":
    unittest.main()
