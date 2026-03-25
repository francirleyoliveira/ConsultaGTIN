from __future__ import annotations

import sqlite3
import unittest
import uuid
from pathlib import Path

from app.services.sqlite_service import ConsultaGtinRepository


class ConsultaGtinRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path("tests") / ".tmp_sqlite_service" / uuid.uuid4().hex
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / "test.db"
        self.repo = ConsultaGtinRepository(self.db_path)

    def test_salva_catalogo_tributario_em_tabelas_persistentes(self) -> None:
        catalogo = [
            {
                "cst": "200",
                "descricao_cst": "Aliquota reduzida",
                "ind_ibscbs": "1",
                "ind_red_bc": "0",
                "ind_red_aliq": "1",
                "ind_transf_cred": "0",
                "ind_dif": "0",
                "ind_ajuste_compet": "0",
                "ind_ibscbs_mono": "0",
                "ind_cred_pres_ibs_zfm": "0",
                "publicacao": "2025-05-12T00:00:00",
                "inicio_vigencia": "2025-05-01T00:00:00",
                "fim_vigencia": "",
                "raw_json": "{}",
                "classificacoes_tributarias": [
                    {
                        "cst": "200",
                        "cclasstrib": "200035",
                        "descricao": "Dentifricios",
                        "p_red_ibs": "60.0",
                        "p_red_cbs": "60.0",
                        "tipo_aliquota": "Padrao",
                        "ind_trib_regular": "0",
                        "ind_cred_pres_oper": "0",
                        "ind_estorno_cred": "0",
                        "monofasia_sujeita_retencao": "0",
                        "monofasia_retida_ant": "0",
                        "monofasia_diferimento": "0",
                        "monofasia_padrao": "0",
                        "ind_nfe": "1",
                        "ind_nfce": "1",
                        "ind_cte": "0",
                        "ind_cteos": "0",
                        "ind_bpe": "0",
                        "ind_nf3e": "0",
                        "ind_nfcom": "0",
                        "ind_nfse": "0",
                        "ind_bpetm": "0",
                        "ind_bpeta": "0",
                        "ind_nfag": "0",
                        "ind_nfsvia": "0",
                        "ind_nfabi": "0",
                        "ind_nfgas": "0",
                        "ind_dere": "0",
                        "anexo": "8",
                        "publicacao": "2025-05-12T00:00:00",
                        "inicio_vigencia": "2025-05-01T00:00:00",
                        "fim_vigencia": "",
                        "base_legal": "https://exemplo.local/base-legal",
                        "links_legais": ["https://exemplo.local/base-legal"],
                        "raw_json": "{}",
                    }
                ],
            }
        ]
        self.repo.salvar_catalogo_tributario(catalogo)
        self.repo.salvar_anexos_tributarios([
            {
                "anexo": "8",
                "descricao": "Anexo de teste",
                "publicacao": "2025-05-12T00:00:00",
                "inicio_vigencia": "2025-05-01T00:00:00",
                "fim_vigencia": "",
                "raw_json": "{}",
                "especificidades": [
                    {
                        "codigo": "ESP1",
                        "descricao": "Especificidade 1",
                        "valor": "ABC",
                        "tipo": "texto",
                        "publicacao": "2025-05-12T00:00:00",
                        "inicio_vigencia": "2025-05-01T00:00:00",
                        "fim_vigencia": "",
                        "raw_json": "{}",
                    }
                ],
            }
        ])

        csts = self.repo.listar_catalogo_cst()
        dossie = self.repo.obter_dossie_cache("200035")
        anexo = self.repo.obter_anexo_cache("8")
        self.assertEqual(1, len(csts))
        self.assertEqual("200", csts[0]["cst"])
        self.assertEqual("200035", dossie["cclasstrib"])
        self.assertEqual("60.0", dossie["p_red_ibs"])
        self.assertEqual("8", dossie["anexo"])
        self.assertEqual("Anexo de teste", anexo["descricao"])
        self.assertEqual(1, len(anexo["especificidades"]))
        self.assertEqual("1", dossie["ind_nfe"])

    def test_salva_consulta_dossie_e_cenarios(self) -> None:
        self.repo.upsert_consulta(
            {
                "cod_winthor": "100",
                "gtin": "7891234567895",
                "data_hora_resposta": "24/03/2026 10:00:00",
                "status_sefaz": "949",
                "motivo_sefaz": "OK",
                "ncm_winthor": "22030000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "OK",
                "descricao_produto": "Produto Teste",
                "cest": "123",
            }
        )
        self.repo.salvar_catalogo_tributario([
            {
                "cst": "060",
                "descricao_cst": "Teste",
                "ind_ibscbs": "1",
                "ind_red_bc": "0",
                "ind_red_aliq": "0",
                "ind_transf_cred": "0",
                "ind_dif": "0",
                "ind_ajuste_compet": "0",
                "ind_ibscbs_mono": "0",
                "ind_cred_pres_ibs_zfm": "0",
                "publicacao": "",
                "inicio_vigencia": "",
                "fim_vigencia": "",
                "raw_json": "{}",
                "classificacoes_tributarias": [
                    {
                        "cst": "060",
                        "cclasstrib": "ABC123",
                        "descricao": "Dossie teste",
                        "p_red_ibs": "12.5",
                        "p_red_cbs": "7.5",
                        "tipo_aliquota": "",
                        "ind_trib_regular": "",
                        "ind_cred_pres_oper": "",
                        "ind_estorno_cred": "",
                        "monofasia_sujeita_retencao": "",
                        "monofasia_retida_ant": "",
                        "monofasia_diferimento": "",
                        "monofasia_padrao": "",
                        "ind_nfe": "S",
                        "ind_nfce": "N",
                        "ind_cte": "",
                        "ind_cteos": "",
                        "ind_bpe": "",
                        "ind_nf3e": "",
                        "ind_nfcom": "",
                        "ind_nfse": "",
                        "ind_bpetm": "",
                        "ind_bpeta": "",
                        "ind_nfag": "",
                        "ind_nfsvia": "",
                        "ind_nfabi": "",
                        "ind_nfgas": "",
                        "ind_dere": "",
                        "anexo": "IV",
                        "publicacao": "2026-03-24T00:00:00",
                        "inicio_vigencia": "2026-04-01T00:00:00",
                        "fim_vigencia": "",
                        "base_legal": "LC 214/25",
                        "links_legais": ["https://exemplo.local/lei"],
                        "raw_json": "{}",
                    }
                ],
            }
        ])
        self.repo.salvar_anexos_tributarios([
            {
                "anexo": "IV",
                "descricao": "Anexo IV",
                "publicacao": "2026-03-24T00:00:00",
                "inicio_vigencia": "2026-04-01T00:00:00",
                "fim_vigencia": "",
                "raw_json": "{}",
                "especificidades": [
                    {
                        "codigo": "ESP2",
                        "descricao": "Faixa especial",
                        "valor": "1",
                        "tipo": "numero",
                        "publicacao": "2026-03-24T00:00:00",
                        "inicio_vigencia": "2026-04-01T00:00:00",
                        "fim_vigencia": "",
                        "raw_json": "{}",
                    }
                ],
            }
        ])
        self.repo.salvar_cenarios_tributarios(
            "22030000",
            [
                {
                    "ncm": "22030000",
                    "cst": "060",
                    "cclasstrib": "ABC123",
                    "condicao_legal": "Uso geral",
                    "descricao_dossie": "Dossie teste",
                    "p_red_ibs": "12.5",
                    "p_red_cbs": "7.5",
                    "publicacao": "2026-03-24T00:00:00",
                    "inicio_vigencia": "2026-04-01T00:00:00",
                    "anexo": "IV",
                    "ind_nfe": "S",
                    "ind_nfce": "N",
                    "base_legal": "LC 214/25",
                    "fonte": "portal_conformidade_facil",
                }
            ],
        )

        consultas = self.repo.listar_consultas()
        cenarios = self.repo.listar_cenarios_tributarios()
        dossie = self.repo.obter_dossie_cache("ABC123")
        anexo = self.repo.obter_anexo_cache("IV")
        resumo = self.repo.obter_resumo_estatistico()

        self.assertEqual(1, len(consultas))
        self.assertEqual("7891234567895", consultas[0]["gtin"])
        self.assertEqual(1, len(cenarios))
        self.assertEqual("22030000", cenarios[0]["ncm"])
        self.assertEqual("060", cenarios[0]["cst"])
        self.assertEqual("ABC123", cenarios[0]["cclasstrib"])
        self.assertEqual("12.5", cenarios[0]["p_red_ibs"])
        self.assertEqual("2026-04-01T00:00:00", cenarios[0]["inicio_vigencia"])
        self.assertEqual("IV", cenarios[0]["anexo"])
        self.assertIsNotNone(dossie)
        self.assertEqual("Dossie teste", dossie["descricao"])
        self.assertEqual("Anexo IV", anexo["descricao"])
        self.assertEqual(1, len(anexo["especificidades"]))
        retorno_anexos = self.repo.listar_retorno_anexos({"anexo": "IV"})
        self.assertEqual(1, len(retorno_anexos))
        self.assertEqual("ESP2", retorno_anexos[0]["codigo_especificidade"])
        self.assertEqual("Faixa especial", retorno_anexos[0]["descricao_especificidade"])
        self.assertEqual(1, resumo["total"])
        self.assertEqual(1, resumo["total_cenarios"])
        self.assertEqual(1, resumo["total_dossies"])
        self.assertEqual(1, resumo["total_anexos"])

    def test_remove_duplicados_antes_de_persistir(self) -> None:
        duplicado = {
            "ncm": "22030000",
            "cst": "060",
            "cclasstrib": "ABC123",
            "condicao_legal": "Uso geral",
            "descricao_dossie": "Dossie teste",
            "p_red_ibs": "12.5",
            "p_red_cbs": "7.5",
            "publicacao": "2026-03-24T00:00:00",
            "inicio_vigencia": "2026-04-01T00:00:00",
            "anexo": "IV",
            "ind_nfe": "S",
            "ind_nfce": "N",
            "base_legal": "LC 214/25",
            "fonte": "portal_conformidade_facil",
        }
        self.repo.salvar_cenarios_tributarios("22030000", [duplicado, duplicado.copy()])
        cenarios = self.repo.listar_cenarios_tributarios()
        self.assertEqual(1, len(cenarios))

    def test_substitui_anexos_ao_sincronizar_servico(self) -> None:
        self.repo.salvar_anexos_tributarios([
            {
                "anexo": "LEGADO",
                "descricao": "Anexo legado",
                "publicacao": "",
                "inicio_vigencia": "",
                "fim_vigencia": "",
                "raw_json": "{}",
                "especificidades": [],
            }
        ])
        self.repo.salvar_anexos_tributarios(
            [
                {
                    "anexo": "NOVO",
                    "descricao": "Anexo novo",
                    "publicacao": "",
                    "inicio_vigencia": "",
                    "fim_vigencia": "",
                    "raw_json": "{}",
                    "especificidades": [
                        {
                            "codigo": "ESP-NOVO",
                            "descricao": "Especificidade nova",
                            "valor": "1",
                            "tipo": "numero",
                            "publicacao": "",
                            "inicio_vigencia": "",
                            "fim_vigencia": "",
                            "raw_json": "{}",
                        }
                    ],
                }
            ],
            substituir=True,
        )

        self.assertIsNone(self.repo.obter_anexo_cache("LEGADO"))
        self.assertEqual("NOVO", self.repo.obter_anexo_cache("NOVO")["anexo"])

    def test_migra_cenarios_legados_sem_perder_dados(self) -> None:
        db_path = self.temp_dir / "legacy_cenarios.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE consultas_gtin (
                    gtin TEXT PRIMARY KEY,
                    cod_winthor TEXT,
                    data_hora_resposta TEXT,
                    status_sefaz TEXT,
                    motivo_sefaz TEXT,
                    ncm_winthor TEXT,
                    ncm_oficial TEXT,
                    divergencia_ncm TEXT,
                    descricao_produto TEXT,
                    cest TEXT,
                    ultima_atualizacao TEXT NOT NULL,
                    ultima_atualizacao_ordem TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "INSERT INTO consultas_gtin (gtin, ncm_winthor, ncm_oficial, ultima_atualizacao, ultima_atualizacao_ordem) VALUES (?, ?, ?, ?, ?)",
                ("7891234567895", "22030000", "22030000", "24/03/2026 10:00:00", "2026-03-24 10:00:00"),
            )
            conn.execute(
                """
                CREATE TABLE cenarios_tributarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gtin TEXT,
                    cst TEXT,
                    cclasstrib TEXT NOT NULL,
                    condicao_legal TEXT,
                    descricao_dossie TEXT,
                    ind_nfe TEXT,
                    ind_nfce TEXT,
                    base_legal TEXT,
                    fonte TEXT,
                    ultima_atualizacao TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO cenarios_tributarios (gtin, cst, cclasstrib, condicao_legal, descricao_dossie, ind_nfe, ind_nfce, base_legal, fonte, ultima_atualizacao) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("7891234567895", "060", "ABC123", "Uso geral", "Dossie legado", "S", "N", "LC 214/25", "portal", "24/03/2026 10:00:00"),
            )
            conn.commit()

        repo = ConsultaGtinRepository(db_path)
        cenarios = repo.listar_cenarios_tributarios()

        self.assertEqual(1, len(cenarios))
        self.assertEqual("22030000", cenarios[0]["ncm"])
        self.assertEqual("ABC123", cenarios[0]["cclasstrib"])
        self.assertEqual("Dossie legado", cenarios[0]["descricao_dossie"])

    def test_adiciona_colunas_novas_ao_dossie_sem_apagar_cache(self) -> None:
        db_path = self.temp_dir / "legacy_dossie.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "CREATE TABLE dossie_classtrib (cclasstrib TEXT PRIMARY KEY, cst TEXT, descricao TEXT, ind_nfe TEXT, base_legal TEXT)"
            )
            conn.execute(
                "INSERT INTO dossie_classtrib (cclasstrib, cst, descricao, ind_nfe, base_legal) VALUES (?, ?, ?, ?, ?)",
                ("OLD001", "060", "Dossie legado", "1", "LC 214/25"),
            )
            conn.commit()

        repo = ConsultaGtinRepository(db_path)
        dossie = repo.obter_dossie_cache("OLD001")

        self.assertEqual("Dossie legado", dossie["descricao"])
        self.assertIn("tipo_aliquota", dossie)
        self.assertEqual("", dossie["tipo_aliquota"])


if __name__ == "__main__":
    unittest.main()
