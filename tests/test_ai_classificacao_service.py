from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from app.config import Settings
from app.services.ai_classificacao_service import AIClassificationService
from app.services.sqlite_service import ConsultaGtinRepository


class AIClassificationServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path("tests") / ".tmp_ai_classificacao_service" / uuid.uuid4().hex
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
        self.repo.upsert_consulta(
            {
                "cod_winthor": "654",
                "gtin": "7891234567895",
                "data_hora_resposta": "01/04/2026 10:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "Consulta realizada com sucesso",
                "ncm_winthor": "33061000",
                "ncm_oficial": "33061000",
                "divergencia_ncm": "OK",
                "descricao_produto": "Creme dental protecao total",
                "descricao_erp": "Creme dental faixa especial adulto",
                "cest": "1234567",
            }
        )
        self.repo.upsert_consulta(
            {
                "cod_winthor": "655",
                "gtin": "7891234567896",
                "data_hora_resposta": "01/04/2026 10:30:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "Consulta realizada com sucesso",
                "ncm_winthor": "33061000",
                "ncm_oficial": "33061000",
                "divergencia_ncm": "OK",
                "descricao_produto": "Creme dental infantil",
                "descricao_erp": "Creme dental kids suave",
                "cest": "7654321",
            }
        )
        self.repo.salvar_anexos_tributarios(
            [
                {
                    "anexo": "IV",
                    "descricao": "Anexo IV de higiene pessoal",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "fim_vigencia": "",
                    "raw_json": "{}",
                    "especificidades": [
                        {
                            "codigo": "ESP-01",
                            "descricao": "Faixa especial para uso adulto",
                            "valor": "Adulto",
                            "tipo": "texto",
                            "publicacao": "2026-03-20T00:00:00",
                            "inicio_vigencia": "2026-04-01T00:00:00",
                            "fim_vigencia": "",
                            "raw_json": "{}",
                        },
                        {
                            "codigo": "ESP-02",
                            "descricao": "Linha premium",
                            "valor": "Premium",
                            "tipo": "texto",
                            "publicacao": "2026-03-20T00:00:00",
                            "inicio_vigencia": "2026-04-01T00:00:00",
                            "fim_vigencia": "",
                            "raw_json": "{}",
                        }
                    ],
                }
            ]
        )
        self.repo.salvar_catalogo_tributario(
            [
                {
                    "cst": "200",
                    "descricao_cst": "Aliquota reduzida",
                    "ind_ibscbs": "1",
                    "ind_red_bc": "0",
                    "ind_red_aliq": "0",
                    "ind_transf_cred": "0",
                    "ind_dif": "0",
                    "ind_ajuste_compet": "0",
                    "ind_ibscbs_mono": "0",
                    "ind_cred_pres_ibs_zfm": "0",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "fim_vigencia": "",
                    "raw_json": "{}",
                    "classificacoes_tributarias": [
                        {
                            "cst": "200",
                            "cclasstrib": "200035",
                            "descricao": "Cremes dentais e produtos correlatos premium",
                            "p_red_ibs": "60.0",
                            "p_red_cbs": "60.0",
                            "anexo": "IV",
                            "ind_nfe": "1",
                            "ind_nfce": "1",
                            "base_legal": "LC 214/25",
                            "raw_json": "{}",
                        }
                    ],
                },
                {
                    "cst": "000",
                    "descricao_cst": "Integral",
                    "ind_ibscbs": "1",
                    "ind_red_bc": "0",
                    "ind_red_aliq": "0",
                    "ind_transf_cred": "0",
                    "ind_dif": "0",
                    "ind_ajuste_compet": "0",
                    "ind_ibscbs_mono": "0",
                    "ind_cred_pres_ibs_zfm": "0",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "fim_vigencia": "",
                    "raw_json": "{}",
                    "classificacoes_tributarias": [
                        {
                            "cst": "000",
                            "cclasstrib": "000001",
                            "descricao": "Situacoes tributadas integralmente",
                            "anexo": "",
                            "ind_nfe": "1",
                            "ind_nfce": "1",
                            "base_legal": "LC 214/25",
                            "raw_json": "{}",
                        }
                    ],
                }
            ]
        )
        self.repo.salvar_cenarios_tributarios(
            "33061000",
            [
                {
                    "ncm": "33061000",
                    "cst": "200",
                    "cclasstrib": "200035",
                    "condicao_legal": "Produtos de higiene bucal para uso adulto",
                    "descricao_dossie": "Cremes dentais e produtos correlatos premium",
                    "p_red_ibs": "60.0",
                    "p_red_cbs": "60.0",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "anexo": "IV",
                    "ind_nfe": "1",
                    "ind_nfce": "1",
                    "base_legal": "LC 214/25",
                    "fonte": "portal_conformidade_facil",
                },
                {
                    "ncm": "33061000",
                    "cst": "000",
                    "cclasstrib": "000001",
                    "condicao_legal": "Tributacao integral",
                    "descricao_dossie": "Situacoes tributadas integralmente",
                    "p_red_ibs": "0.0",
                    "p_red_cbs": "0.0",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "anexo": "",
                    "ind_nfe": "1",
                    "ind_nfce": "1",
                    "base_legal": "LC 214/25",
                    "fonte": "portal_conformidade_facil",
                }
            ],
        )

    def test_analise_heuristica_considera_anexos_e_particularidades(self) -> None:
        service = AIClassificationService(self.settings, self.repo)

        resultado = service.analisar_gtin("7891234567895")

        analise = resultado["analise"]
        payload = analise["resultado_json"]
        recomendacao = payload["cenario_recomendado"]

        self.assertEqual("heuristic", analise["provider"])
        self.assertEqual("200", analise["recomendacao_cst"])
        self.assertEqual("200035", analise["recomendacao_cclasstrib"])
        self.assertEqual("S", analise["requer_revisao_humana"])
        self.assertEqual("IV", recomendacao["anexo"])
        self.assertTrue(recomendacao["especificidades_relevantes"])
        self.assertTrue(payload["anexos_considerados"])
        self.assertIn("A analise considerou 1 anexo(s)", payload["resumo"])

        feedback_id = service.registrar_feedback(
            analise_id=analise["id"],
            decisao="CONFIRMADO",
            cst_final="200",
            cclasstrib_final="200035",
            observacao="Aderente ao produto e ao anexo.",
        )
        self.assertGreater(feedback_id, 0)
        feedbacks = self.repo.listar_feedback_analise_ia(analise["id"])
        self.assertEqual(1, len(feedbacks))
        self.assertEqual("CONFIRMADO", feedbacks[0]["decisao"])


    def test_analise_por_ncm_marca_contexto_agregado(self) -> None:
        service = AIClassificationService(self.settings, self.repo)

        resultado = service.analisar_ncm("33061000")

        payload = resultado["analise"]["resultado_json"]
        produto = resultado["contexto"]["produto"]

        self.assertTrue(produto["contexto_agregado"])
        self.assertIn("Analise por NCM baseada em contexto agregado", payload["inconsistencias"][0])
        self.assertTrue(any("amostras de produtos do ERP" in item for item in payload["dados_faltantes"]))
        self.assertTrue(any("Contexto agregado por NCM" in item for item in payload["cenario_recomendado"]["restricoes_do_anexo"]))


if __name__ == "__main__":
    unittest.main()
