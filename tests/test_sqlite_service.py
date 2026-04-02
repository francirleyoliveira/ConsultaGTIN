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
                "descricao_erp": "Produto ERP",
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
                    "descricao_dossie": "Valor do cenario nao deve prevalecer",
                    "p_red_ibs": "99.9",
                    "p_red_cbs": "99.9",
                    "publicacao": "2099-01-01T00:00:00",
                    "inicio_vigencia": "2099-01-01T00:00:00",
                    "anexo": "IGNORAR",
                    "ind_nfe": "N",
                    "ind_nfce": "N",
                    "base_legal": "IGNORAR",
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

        consultas_filtradas = self.repo.listar_consultas({"cod_winthor": "100"})
        self.assertEqual(1, len(consultas_filtradas))
        self.assertEqual("7891234567895", consultas_filtradas[0]["gtin"])
        self.assertEqual("Produto ERP", consultas[0]["descricao_erp"])
        self.assertEqual(1, len(cenarios))
        self.assertEqual("22030000", cenarios[0]["ncm"])
        self.assertEqual("060", cenarios[0]["cst"])
        self.assertEqual("ABC123", cenarios[0]["cclasstrib"])
        self.assertNotIn("descricao_erp", cenarios[0])
        self.assertEqual("Dossie teste", cenarios[0]["descricao_dossie"])
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
        self.assertEqual("Faixa especial", retorno_anexos[0]["descr_item_anexo"])
        self.assertEqual("Faixa especial", retorno_anexos[0]["descricao_especificidade"])
        self.assertEqual(1, resumo["total"])
        self.assertEqual(1, resumo["ok"])
        self.assertEqual(1, resumo["total_cenarios"])
        self.assertEqual(1, resumo["total_dossies"])
        self.assertEqual(1, resumo["total_anexos"])

    def test_sincroniza_dados_do_erp_sem_consultar_sefaz_novamente(self) -> None:
        self.repo.upsert_consulta(
            {
                "cod_winthor": "100",
                "gtin": "7891234567895",
                "data_hora_resposta": "24/03/2026 10:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "Consulta realizada com sucesso",
                "ncm_winthor": "22041000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "DIVERGENTE: ERP(22041000) != GS1(22030000)",
                "descricao_produto": "Produto GS1",
                "descricao_erp": "",
                "cest": "123",
            }
        )

        resumo = self.repo.sincronizar_consultas_com_erp([("200", "7891234567895", "22030000", "Produto ERP atualizado")])
        consulta = self.repo.obter_consulta_por_gtin("7891234567895")

        self.assertEqual(1, resumo["atualizados"])
        self.assertEqual(1, resumo["recalculados"])
        self.assertEqual("200", consulta["cod_winthor"])
        self.assertEqual("22030000", consulta["ncm_winthor"])
        self.assertEqual("Produto ERP atualizado", consulta["descricao_erp"])
        self.assertEqual("OK", consulta["divergencia_ncm"])

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
        self.assertNotIn("descricao_erp", cenarios[0])
        self.assertEqual("Dossie legado", cenarios[0]["descricao_dossie"])
        self.assertEqual("Dossie legado", repo.obter_dossie_cache("ABC123")["descricao"])

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

    def test_resumo_nao_conta_status_legado_nao_como_ok(self) -> None:
        self.repo.upsert_consulta(
            {
                "cod_winthor": "100",
                "gtin": "7891234567895",
                "data_hora_resposta": "24/03/2026 10:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "OK",
                "ncm_winthor": "22030000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "NAO",
                "descricao_produto": "Produto legado",
                "descricao_erp": "Produto legado ERP",
                "cest": "123",
            }
        )

        resumo = self.repo.obter_resumo_estatistico()

        self.assertEqual(1, resumo["total"])
        self.assertEqual(0, resumo["ok"])

    def test_listar_retorno_anexos_agrega_referencias_de_cenarios_por_anexo(self) -> None:
        self.repo.salvar_anexos_tributarios([
            {
                "anexo": "VIII",
                "descricao": "Anexo VIII",
                "publicacao": "",
                "inicio_vigencia": "",
                "fim_vigencia": "",
                "raw_json": "{}",
                "especificidades": [
                    {
                        "codigo": "ESP-1",
                        "descricao": "Especificidade 1",
                        "valor": "A",
                        "tipo": "texto",
                        "publicacao": "",
                        "inicio_vigencia": "",
                        "fim_vigencia": "",
                        "raw_json": "{}",
                    }
                ],
            }
        ])
        self.repo.salvar_catalogo_tributario([
            {
                "cst": "060",
                "descricao_cst": "CST A",
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
                        "cclasstrib": "AAA001",
                        "descricao": "Dossie A",
                        "anexo": "VIII",
                        "raw_json": "{}",
                    }
                ],
            },
            {
                "cst": "061",
                "descricao_cst": "CST B",
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
                        "cst": "061",
                        "cclasstrib": "BBB002",
                        "descricao": "Dossie B",
                        "anexo": "VIII",
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
                    "cclasstrib": "AAA001",
                    "condicao_legal": "Uso geral",
                    "fonte": "portal",
                }
            ],
        )
        self.repo.salvar_cenarios_tributarios(
            "22040000",
            [
                {
                    "ncm": "22040000",
                    "cst": "061",
                    "cclasstrib": "BBB002",
                    "condicao_legal": "Uso especifico",
                    "fonte": "portal",
                }
            ],
        )
        retorno = self.repo.listar_retorno_anexos({"anexo": "VIII"})

        self.assertEqual(1, len(retorno))
        self.assertEqual(2, retorno[0]["total_ncms_relacionados"])
        self.assertEqual(2, retorno[0]["total_cenarios_relacionados"])
        self.assertEqual("22030000,22040000", retorno[0]["ncms_relacionados"])
        self.assertEqual("060,061", retorno[0]["csts_relacionados"])
        self.assertEqual("AAA001,BBB002", retorno[0]["cclasstrib_relacionados"])
        self.assertEqual("ESP-1", retorno[0]["codigo_especificidade"])


    def test_salvar_feedback_analise_ia_rejeita_analise_inexistente(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.salvar_feedback_analise_ia(9999, "CONFIRMADO")


    def test_salvar_cenarios_persiste_base_mesmo_sem_dossie_previo(self) -> None:
        self.repo.salvar_cenarios_tributarios(
            "33061000",
            [
                {
                    "ncm": "33061000",
                    "cst": "200",
                    "cclasstrib": "200035",
                    "condicao_legal": "Uso geral",
                    "fonte": "portal",
                }
            ],
        )

        dossie = self.repo.obter_dossie_cache("200035")
        cenarios = self.repo.listar_cenarios_tributarios({"ncm": "33061000"})

        self.assertIsNone(dossie)
        self.assertEqual(1, len(cenarios))
        self.assertEqual("", cenarios[0]["descricao_dossie"])
        self.assertEqual("", cenarios[0]["anexo"])


    def test_listar_consultas_por_ncm_retorna_mais_recentes(self) -> None:
        self.repo.upsert_consulta(
            {
                "cod_winthor": "10",
                "gtin": "7891234567895",
                "data_hora_resposta": "24/03/2026 10:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "OK",
                "ncm_winthor": "22030000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "OK",
                "descricao_produto": "Produto 1",
                "descricao_erp": "ERP 1",
                "cest": "123",
            }
        )
        self.repo.upsert_consulta(
            {
                "cod_winthor": "11",
                "gtin": "7891234567896",
                "data_hora_resposta": "25/03/2026 11:00:00",
                "status_sefaz": "9490",
                "motivo_sefaz": "OK",
                "ncm_winthor": "22030000",
                "ncm_oficial": "22030000",
                "divergencia_ncm": "OK",
                "descricao_produto": "Produto 2",
                "descricao_erp": "ERP 2",
                "cest": "124",
            }
        )

        consultas = self.repo.listar_consultas_por_ncm("22030000", limit=1)

        self.assertEqual(1, len(consultas))
        self.assertEqual("7891234567896", consultas[0]["gtin"])


    def test_migracao_datas_ordenacao_atualiza_apenas_registros_sem_ordem(self) -> None:
        db_path = self.temp_dir / "migration.db"
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
                    descricao_erp TEXT,
                    cest TEXT,
                    ultima_atualizacao TEXT NOT NULL,
                    ultima_atualizacao_ordem TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "INSERT INTO consultas_gtin (gtin, ultima_atualizacao, ultima_atualizacao_ordem) VALUES (?, ?, ?)",
                ("111", "02/04/2026 10:00:00", ""),
            )
            conn.execute(
                "INSERT INTO consultas_gtin (gtin, ultima_atualizacao, ultima_atualizacao_ordem) VALUES (?, ?, ?)",
                ("222", "01/04/2026 09:00:00", "2026-04-01 09:00:00"),
            )
            conn.commit()

        repo = ConsultaGtinRepository(db_path)
        with repo._managed_conn() as conn:
            rows = conn.execute(
                "SELECT gtin, ultima_atualizacao_ordem FROM consultas_gtin ORDER BY gtin ASC"
            ).fetchall()

        self.assertEqual("2026-04-02 10:00:00", rows[0]["ultima_atualizacao_ordem"])
        self.assertEqual("2026-04-01 09:00:00", rows[1]["ultima_atualizacao_ordem"])


if __name__ == "__main__":
    unittest.main()
