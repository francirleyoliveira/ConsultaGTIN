from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from app.services.ai_context_service import AIContextService
from app.services.sqlite_service import ConsultaGtinRepository


class AIContextServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path("tests") / ".tmp_ai_context_service" / uuid.uuid4().hex
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.repo = ConsultaGtinRepository(self.temp_dir / "test.db")
        self.repo.upsert_consulta(
            {
                "cod_winthor": "321",
                "gtin": "7891234567895",
                "data_hora_resposta": "01/04/2026 09:00:00",
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
                            "descricao": "Cremes dentais e produtos correlatos",
                            "p_red_ibs": "60.0",
                            "p_red_cbs": "60.0",
                            "anexo": "IV",
                            "ind_nfe": "1",
                            "ind_nfce": "1",
                            "base_legal": "LC 214/25",
                            "raw_json": "{}",
                        }
                    ],
                }
            ]
        )
        self.repo.upsert_consulta(
            {
                "cod_winthor": "654",
                "gtin": "7891234567896",
                "data_hora_resposta": "01/04/2026 10:00:00",
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
        self.repo.salvar_cenarios_tributarios(
            "33061000",
            [
                {
                    "ncm": "33061000",
                    "cst": "200",
                    "cclasstrib": "200035",
                    "condicao_legal": "Produtos de higiene bucal",
                    "descricao_dossie": "Cremes dentais e produtos correlatos",
                    "p_red_ibs": "60.0",
                    "p_red_cbs": "60.0",
                    "publicacao": "2026-03-20T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "anexo": "IV",
                    "ind_nfe": "1",
                    "ind_nfce": "1",
                    "base_legal": "LC 214/25",
                    "fonte": "portal_conformidade_facil",
                }
            ],
        )

    def test_monta_contexto_com_anexo_e_particularidades(self) -> None:
        service = AIContextService(self.repo)

        contexto = service.montar_contexto_por_gtin("7891234567895")

        self.assertEqual("GTIN", contexto["tipo_contexto"])
        self.assertEqual("7891234567895", contexto["chave_contexto"])
        self.assertEqual("Creme dental faixa especial adulto", contexto["produto"]["descricao_erp"])
        self.assertEqual(1, len(contexto["cenarios"]))
        self.assertEqual("IV", contexto["cenarios"][0]["anexo"])
        self.assertEqual(1, len(contexto["cenarios"][0]["particularidades_anexo"]))
        self.assertEqual("ESP-01", contexto["cenarios"][0]["particularidades_anexo"][0]["codigo"])
        self.assertEqual(1, len(contexto["anexos_considerados"]))
        self.assertEqual("IV", contexto["anexos_considerados"][0]["anexo"])
        self.assertTrue(contexto["contexto_hash"])


    def test_monta_contexto_agregado_por_ncm_sem_fixar_produto_unico(self) -> None:
        service = AIContextService(self.repo)

        contexto = service.montar_contexto_por_ncm("33061000")

        self.assertEqual("NCM", contexto["tipo_contexto"])
        self.assertTrue(contexto["produto"]["contexto_agregado"])
        self.assertEqual(2, contexto["produto"]["quantidade_produtos_contexto"])
        self.assertEqual("MULTIPLOS", contexto["produto"]["cod_winthor"])
        self.assertEqual("", contexto["produto"]["gtin"])
        self.assertIn("Creme dental faixa especial adulto", contexto["produto"]["descricao_erp"])
        self.assertIn("Creme dental kids suave", contexto["produto"]["descricao_erp"])
        self.assertEqual(2, len(contexto["consultas_relacionadas"]))
        self.assertTrue(contexto["resumo_operacional"]["contexto_agregado"])


if __name__ == "__main__":
    unittest.main()
