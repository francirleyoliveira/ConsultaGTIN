from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import SQLITE_DB_PATH, ensure_output_dirs
from app.validators.gtin import comparar_ncm


CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS consultas_gtin (
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
    """,
    """
    CREATE TABLE IF NOT EXISTS catalogo_cst_tributario (
        cst TEXT PRIMARY KEY,
        descricao_cst TEXT,
        ind_ibscbs TEXT,
        ind_red_bc TEXT,
        ind_red_aliq TEXT,
        ind_transf_cred TEXT,
        ind_dif TEXT,
        ind_ajuste_compet TEXT,
        ind_ibscbs_mono TEXT,
        ind_cred_pres_ibs_zfm TEXT,
        publicacao TEXT,
        inicio_vigencia TEXT,
        fim_vigencia TEXT,
        raw_json TEXT,
        ultima_atualizacao TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dossie_classtrib (
        cclasstrib TEXT PRIMARY KEY,
        cst TEXT,
        descricao TEXT,
        p_red_ibs TEXT,
        p_red_cbs TEXT,
        tipo_aliquota TEXT,
        ind_trib_regular TEXT,
        ind_cred_pres_oper TEXT,
        ind_estorno_cred TEXT,
        monofasia_sujeita_retencao TEXT,
        monofasia_retida_ant TEXT,
        monofasia_diferimento TEXT,
        monofasia_padrao TEXT,
        ind_nfe TEXT,
        ind_nfce TEXT,
        ind_cte TEXT,
        ind_cteos TEXT,
        ind_bpe TEXT,
        ind_nf3e TEXT,
        ind_nfcom TEXT,
        ind_nfse TEXT,
        ind_bpetm TEXT,
        ind_bpeta TEXT,
        ind_nfag TEXT,
        ind_nfsvia TEXT,
        ind_nfabi TEXT,
        ind_nfgas TEXT,
        ind_dere TEXT,
        anexo TEXT,
        publicacao TEXT,
        inicio_vigencia TEXT,
        fim_vigencia TEXT,
        base_legal TEXT,
        links_legais TEXT,
        raw_json TEXT,
        ultima_atualizacao TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anexos_tributarios (
        anexo TEXT PRIMARY KEY,
        descricao TEXT,
        publicacao TEXT,
        inicio_vigencia TEXT,
        fim_vigencia TEXT,
        raw_json TEXT,
        ultima_atualizacao TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anexos_especificidades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        anexo TEXT NOT NULL,
        codigo TEXT,
        descricao TEXT,
        valor TEXT,
        tipo TEXT,
        publicacao TEXT,
        inicio_vigencia TEXT,
        fim_vigencia TEXT,
        raw_json TEXT,
        ultima_atualizacao TEXT NOT NULL,
        UNIQUE(anexo, codigo, descricao, valor),
        FOREIGN KEY(anexo) REFERENCES anexos_tributarios(anexo)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_analises (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo_contexto TEXT NOT NULL,
        chave_contexto TEXT NOT NULL,
        contexto_hash TEXT NOT NULL,
        origem_contexto TEXT,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        prompt_version TEXT NOT NULL,
        status_execucao TEXT NOT NULL,
        score_confianca REAL,
        recomendacao_cst TEXT,
        recomendacao_cclasstrib TEXT,
        requer_revisao_humana TEXT NOT NULL DEFAULT 'S',
        resumo TEXT,
        resultado_json TEXT NOT NULL,
        criado_em TEXT NOT NULL,
        atualizado_em TEXT NOT NULL,
        UNIQUE(tipo_contexto, chave_contexto, contexto_hash, prompt_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_feedback_analista (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        analise_id INTEGER NOT NULL,
        decisao TEXT NOT NULL,
        cst_final TEXT,
        cclasstrib_final TEXT,
        observacao TEXT,
        criado_em TEXT NOT NULL,
        FOREIGN KEY(analise_id) REFERENCES ai_analises(id)
    )
    """,
]


CREATE_CENARIOS_SQL = """
CREATE TABLE IF NOT EXISTS cenarios_ncm_base (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ncm TEXT NOT NULL,
    cst TEXT,
    cclasstrib TEXT NOT NULL,
    condicao_legal TEXT,
    fonte TEXT,
    ultima_atualizacao TEXT NOT NULL,
    UNIQUE(ncm, cclasstrib, cst, condicao_legal)
)
"""


CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_consultas_status ON consultas_gtin(status_sefaz)",
    "CREATE INDEX IF NOT EXISTS idx_consultas_ncm_winthor ON consultas_gtin(ncm_winthor)",
    "CREATE INDEX IF NOT EXISTS idx_consultas_ncm_oficial ON consultas_gtin(ncm_oficial)",
    "CREATE INDEX IF NOT EXISTS idx_consultas_atualizacao ON consultas_gtin(ultima_atualizacao_ordem)",
    "CREATE INDEX IF NOT EXISTS idx_consultas_ncm_atualizacao ON consultas_gtin(ncm_winthor, ultima_atualizacao_ordem DESC)",
    "CREATE INDEX IF NOT EXISTS idx_catalogo_cst_publicacao ON catalogo_cst_tributario(publicacao)",
    "CREATE INDEX IF NOT EXISTS idx_dossie_cst ON dossie_classtrib(cst)",
    "CREATE INDEX IF NOT EXISTS idx_anexos_publicacao ON anexos_tributarios(publicacao)",
    "CREATE INDEX IF NOT EXISTS idx_anexos_especificidades_anexo ON anexos_especificidades(anexo)",
    "CREATE INDEX IF NOT EXISTS idx_cenarios_base_ncm ON cenarios_ncm_base(ncm)",
    "CREATE INDEX IF NOT EXISTS idx_cenarios_base_cst ON cenarios_ncm_base(cst)",
    "CREATE INDEX IF NOT EXISTS idx_cenarios_base_cclasstrib ON cenarios_ncm_base(cclasstrib)",
    "CREATE INDEX IF NOT EXISTS idx_cenarios_base_ncm_cst ON cenarios_ncm_base(ncm, cst)",
    "CREATE INDEX IF NOT EXISTS idx_cenarios_base_ncm_cclasstrib ON cenarios_ncm_base(ncm, cclasstrib)",
    "CREATE INDEX IF NOT EXISTS idx_ai_analises_contexto ON ai_analises(tipo_contexto, chave_contexto, atualizado_em)",
    "CREATE INDEX IF NOT EXISTS idx_ai_feedback_analise ON ai_feedback_analista(analise_id)",
]


CENARIOS_COLUMN_DEFS = [
    ("ncm", "TEXT NOT NULL DEFAULT ''"),
    ("cst", "TEXT DEFAULT ''"),
    ("cclasstrib", "TEXT NOT NULL DEFAULT ''"),
    ("condicao_legal", "TEXT"),
    ("fonte", "TEXT"),
    ("ultima_atualizacao", "TEXT NOT NULL DEFAULT ''"),
]


DOSSIE_COLUMN_DEFS = [
    ("cst", "TEXT DEFAULT ''"),
    ("descricao", "TEXT DEFAULT ''"),
    ("p_red_ibs", "TEXT DEFAULT ''"),
    ("p_red_cbs", "TEXT DEFAULT ''"),
    ("tipo_aliquota", "TEXT DEFAULT ''"),
    ("ind_trib_regular", "TEXT DEFAULT ''"),
    ("ind_cred_pres_oper", "TEXT DEFAULT ''"),
    ("ind_estorno_cred", "TEXT DEFAULT ''"),
    ("monofasia_sujeita_retencao", "TEXT DEFAULT ''"),
    ("monofasia_retida_ant", "TEXT DEFAULT ''"),
    ("monofasia_diferimento", "TEXT DEFAULT ''"),
    ("monofasia_padrao", "TEXT DEFAULT ''"),
    ("ind_nfe", "TEXT DEFAULT ''"),
    ("ind_nfce", "TEXT DEFAULT ''"),
    ("ind_cte", "TEXT DEFAULT ''"),
    ("ind_cteos", "TEXT DEFAULT ''"),
    ("ind_bpe", "TEXT DEFAULT ''"),
    ("ind_nf3e", "TEXT DEFAULT ''"),
    ("ind_nfcom", "TEXT DEFAULT ''"),
    ("ind_nfse", "TEXT DEFAULT ''"),
    ("ind_bpetm", "TEXT DEFAULT ''"),
    ("ind_bpeta", "TEXT DEFAULT ''"),
    ("ind_nfag", "TEXT DEFAULT ''"),
    ("ind_nfsvia", "TEXT DEFAULT ''"),
    ("ind_nfabi", "TEXT DEFAULT ''"),
    ("ind_nfgas", "TEXT DEFAULT ''"),
    ("ind_dere", "TEXT DEFAULT ''"),
    ("anexo", "TEXT DEFAULT ''"),
    ("publicacao", "TEXT DEFAULT ''"),
    ("inicio_vigencia", "TEXT DEFAULT ''"),
    ("fim_vigencia", "TEXT DEFAULT ''"),
    ("base_legal", "TEXT DEFAULT ''"),
    ("links_legais", "TEXT DEFAULT ''"),
    ("raw_json", "TEXT DEFAULT ''"),
    ("ultima_atualizacao", "TEXT NOT NULL DEFAULT ''"),
]


class ConsultaGtinRepository:
    def __init__(self, db_path: Path = SQLITE_DB_PATH):
        ensure_output_dirs()
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _managed_conn(self):
        conn = self._connect()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._managed_conn() as conn:
            for sql in CREATE_TABLES_SQL:
                conn.execute(sql)
            self._garantir_schema_consultas(conn)
            self._garantir_schema_cenarios(conn)
            self._garantir_schema_dossie(conn)
            self._migrar_datas_ordenacao(conn)
            for sql in CREATE_INDEXES_SQL:
                conn.execute(sql)
            conn.commit()

    def _garantir_schema_consultas(self, conn: sqlite3.Connection) -> None:
        colunas = {row[1] for row in conn.execute("PRAGMA table_info(consultas_gtin)").fetchall()}
        if "ultima_atualizacao_ordem" not in colunas:
            conn.execute("ALTER TABLE consultas_gtin ADD COLUMN ultima_atualizacao_ordem TEXT NOT NULL DEFAULT ''")
        if "descricao_erp" not in colunas:
            conn.execute("ALTER TABLE consultas_gtin ADD COLUMN descricao_erp TEXT NOT NULL DEFAULT ''")

    def _garantir_schema_cenarios(self, conn: sqlite3.Connection) -> None:
        conn.execute(CREATE_CENARIOS_SQL)
        existe_legado = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cenarios_tributarios'"
        ).fetchone()
        if existe_legado:
            self._migrar_cenarios_legacy(conn)
        colunas = {row[1] for row in conn.execute("PRAGMA table_info(cenarios_ncm_base)").fetchall()}
        for coluna, definicao in CENARIOS_COLUMN_DEFS:
            if coluna not in colunas:
                conn.execute(f"ALTER TABLE cenarios_ncm_base ADD COLUMN {coluna} {definicao}")

    def _garantir_schema_dossie(self, conn: sqlite3.Connection) -> None:
        colunas = {row[1] for row in conn.execute("PRAGMA table_info(dossie_classtrib)").fetchall()}
        for coluna, definicao in DOSSIE_COLUMN_DEFS:
            if coluna not in colunas:
                conn.execute(f"ALTER TABLE dossie_classtrib ADD COLUMN {coluna} {definicao}")

    def _migrar_cenarios_legacy(self, conn: sqlite3.Connection) -> None:
        legacy = "cenarios_tributarios_legacy"
        conn.execute(f"DROP TABLE IF EXISTS {legacy}")
        conn.execute(f"ALTER TABLE cenarios_tributarios RENAME TO {legacy}")

        colunas_origem = {row[1] for row in conn.execute(f"PRAGMA table_info({legacy})").fetchall()}

        def expr(coluna: str, fallback: str = "''") -> str:
            return f"legacy.{coluna}" if coluna in colunas_origem else fallback

        partes_ncm: list[str] = []
        if "ncm" in colunas_origem:
            partes_ncm.append("legacy.ncm")
        if "gtin" in colunas_origem:
            partes_ncm.extend(["consultas.ncm_oficial", "consultas.ncm_winthor"])
        partes_ncm_sql = partes_ncm + ["''"]
        ncm_expr = f"COALESCE({', '.join(partes_ncm_sql)})" if partes_ncm else "''"
        cclasstrib_expr = expr("cclasstrib")
        cst_expr = expr("cst")
        ultima_atualizacao_expr = expr("ultima_atualizacao", "strftime('%d/%m/%Y %H:%M:%S', 'now')")
        join_consultas = "LEFT JOIN consultas_gtin consultas ON consultas.gtin = legacy.gtin" if "gtin" in colunas_origem else ""

        conn.execute(
            f"""
            INSERT INTO dossie_classtrib (
                cclasstrib, cst, descricao, p_red_ibs, p_red_cbs, publicacao,
                inicio_vigencia, anexo, ind_nfe, ind_nfce, base_legal, raw_json, ultima_atualizacao
            )
            SELECT DISTINCT
                {cclasstrib_expr},
                {cst_expr},
                {expr('descricao_dossie')},
                {expr('p_red_ibs')},
                {expr('p_red_cbs')},
                {expr('publicacao')},
                {expr('inicio_vigencia')},
                {expr('anexo')},
                {expr('ind_nfe')},
                {expr('ind_nfce')},
                {expr('base_legal')},
                '{{}}',
                {ultima_atualizacao_expr}
            FROM {legacy} legacy
            {join_consultas}
            WHERE TRIM({cclasstrib_expr}) <> ''
            ON CONFLICT(cclasstrib) DO UPDATE SET
                cst = CASE WHEN TRIM(COALESCE(dossie_classtrib.cst, '')) = '' THEN excluded.cst ELSE dossie_classtrib.cst END,
                descricao = CASE WHEN TRIM(COALESCE(dossie_classtrib.descricao, '')) = '' THEN excluded.descricao ELSE dossie_classtrib.descricao END,
                p_red_ibs = CASE WHEN TRIM(COALESCE(dossie_classtrib.p_red_ibs, '')) = '' THEN excluded.p_red_ibs ELSE dossie_classtrib.p_red_ibs END,
                p_red_cbs = CASE WHEN TRIM(COALESCE(dossie_classtrib.p_red_cbs, '')) = '' THEN excluded.p_red_cbs ELSE dossie_classtrib.p_red_cbs END,
                publicacao = CASE WHEN TRIM(COALESCE(dossie_classtrib.publicacao, '')) = '' THEN excluded.publicacao ELSE dossie_classtrib.publicacao END,
                inicio_vigencia = CASE WHEN TRIM(COALESCE(dossie_classtrib.inicio_vigencia, '')) = '' THEN excluded.inicio_vigencia ELSE dossie_classtrib.inicio_vigencia END,
                anexo = CASE WHEN TRIM(COALESCE(dossie_classtrib.anexo, '')) = '' THEN excluded.anexo ELSE dossie_classtrib.anexo END,
                ind_nfe = CASE WHEN TRIM(COALESCE(dossie_classtrib.ind_nfe, '')) = '' THEN excluded.ind_nfe ELSE dossie_classtrib.ind_nfe END,
                ind_nfce = CASE WHEN TRIM(COALESCE(dossie_classtrib.ind_nfce, '')) = '' THEN excluded.ind_nfce ELSE dossie_classtrib.ind_nfce END,
                base_legal = CASE WHEN TRIM(COALESCE(dossie_classtrib.base_legal, '')) = '' THEN excluded.base_legal ELSE dossie_classtrib.base_legal END
            """
        )

        conn.execute(
            f"""
            INSERT OR IGNORE INTO cenarios_ncm_base (
                ncm, cst, cclasstrib, condicao_legal, fonte, ultima_atualizacao
            )
            SELECT
                {ncm_expr},
                {cst_expr},
                {cclasstrib_expr},
                {expr('condicao_legal')},
                {expr('fonte')},
                {ultima_atualizacao_expr}
            FROM {legacy} legacy
            {join_consultas}
            WHERE TRIM({cclasstrib_expr}) <> ''
              AND TRIM({ncm_expr}) <> ''
            """
        )
        conn.execute(f"DROP TABLE {legacy}")

    def _migrar_datas_ordenacao(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            "SELECT gtin, ultima_atualizacao, ultima_atualizacao_ordem FROM consultas_gtin"
        ).fetchall()
        for row in rows:
            if row["ultima_atualizacao_ordem"]:
                continue
            iso = self._converter_data_para_ordem(row["ultima_atualizacao"])
            conn.execute(
                "UPDATE consultas_gtin SET ultima_atualizacao_ordem = ? WHERE gtin = ?",
                (iso, row["gtin"]),
            )

    def _converter_data_para_ordem(self, data_texto: str | None) -> str:
        if not data_texto:
            return ""
        try:
            return datetime.strptime(data_texto, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return str(data_texto)

    def upsert_consulta(self, registro: dict[str, Any]) -> None:
        agora = datetime.now()
        payload = {
            "gtin": str(registro.get("gtin", "")).strip(),
            "cod_winthor": str(registro.get("cod_winthor", "") or ""),
            "data_hora_resposta": registro.get("data_hora_resposta", "") or "",
            "status_sefaz": registro.get("status_sefaz", "") or "",
            "motivo_sefaz": registro.get("motivo_sefaz", "") or "",
            "ncm_winthor": registro.get("ncm_winthor", "") or "",
            "ncm_oficial": registro.get("ncm_oficial", "") or "",
            "divergencia_ncm": registro.get("divergencia_ncm", "") or "",
            "descricao_produto": registro.get("descricao_produto", "") or "",
            "descricao_erp": registro.get("descricao_erp", "") or "",
            "cest": registro.get("cest", "") or "",
            "ultima_atualizacao": agora.strftime("%d/%m/%Y %H:%M:%S"),
            "ultima_atualizacao_ordem": agora.strftime("%Y-%m-%d %H:%M:%S"),
        }
        with self._managed_conn() as conn:
            conn.execute(
                """
                INSERT INTO consultas_gtin (
                    gtin, cod_winthor, data_hora_resposta, status_sefaz, motivo_sefaz,
                    ncm_winthor, ncm_oficial, divergencia_ncm, descricao_produto, descricao_erp, cest,
                    ultima_atualizacao, ultima_atualizacao_ordem
                ) VALUES (
                    :gtin, :cod_winthor, :data_hora_resposta, :status_sefaz, :motivo_sefaz,
                    :ncm_winthor, :ncm_oficial, :divergencia_ncm, :descricao_produto, :descricao_erp, :cest,
                    :ultima_atualizacao, :ultima_atualizacao_ordem
                )
                ON CONFLICT(gtin) DO UPDATE SET
                    cod_winthor = excluded.cod_winthor,
                    data_hora_resposta = excluded.data_hora_resposta,
                    status_sefaz = excluded.status_sefaz,
                    motivo_sefaz = excluded.motivo_sefaz,
                    ncm_winthor = excluded.ncm_winthor,
                    ncm_oficial = excluded.ncm_oficial,
                    divergencia_ncm = excluded.divergencia_ncm,
                    descricao_produto = excluded.descricao_produto,
                    descricao_erp = excluded.descricao_erp,
                    cest = excluded.cest,
                    ultima_atualizacao = excluded.ultima_atualizacao,
                    ultima_atualizacao_ordem = excluded.ultima_atualizacao_ordem
                """,
                payload,
            )
            conn.commit()

    def sincronizar_consultas_com_erp(self, produtos_erp: list[tuple[Any, ...]], chunk_size: int = 900) -> dict[str, int]:
        produtos_por_gtin: dict[str, dict[str, str]] = {}
        for produto in produtos_erp:
            gtin = str(produto[1] or "").strip() if len(produto) > 1 else ""
            if not gtin:
                continue
            produtos_por_gtin[gtin] = {
                "cod_winthor": str(produto[0] or "") if len(produto) > 0 else "",
                "ncm_winthor": "".join(filter(str.isdigit, str(produto[2] or ""))) if len(produto) > 2 else "",
                "descricao_erp": str(produto[3] or "") if len(produto) > 3 else "",
            }

        if not produtos_por_gtin:
            return {"recebidos": 0, "atualizados": 0, "recalculados": 0}

        gtins = list(produtos_por_gtin.keys())
        atualizados = 0
        recalculados = 0

        with self._managed_conn() as conn:
            for inicio in range(0, len(gtins), chunk_size):
                lote = gtins[inicio : inicio + chunk_size]
                placeholders = ",".join("?" for _ in lote)
                rows = conn.execute(
                    f"""
                    SELECT gtin, ncm_oficial, divergencia_ncm
                    FROM consultas_gtin
                    WHERE gtin IN ({placeholders})
                    """,
                    lote,
                ).fetchall()

                for row in rows:
                    gtin = str(row["gtin"] or "")
                    dados_erp = produtos_por_gtin[gtin]
                    ncm_oficial = "".join(filter(str.isdigit, str(row["ncm_oficial"] or "")))
                    divergencia_nova = str(row["divergencia_ncm"] or "")
                    if ncm_oficial:
                        divergencia_nova = comparar_ncm(dados_erp["ncm_winthor"], ncm_oficial)
                        recalculados += 1

                    conn.execute(
                        """
                        UPDATE consultas_gtin
                        SET cod_winthor = ?,
                            ncm_winthor = ?,
                            descricao_erp = ?,
                            divergencia_ncm = ?
                        WHERE gtin = ?
                        """,
                        (
                            dados_erp["cod_winthor"],
                            dados_erp["ncm_winthor"],
                            dados_erp["descricao_erp"],
                            divergencia_nova,
                            gtin,
                        ),
                    )
                    atualizados += 1

            conn.commit()

        return {
            "recebidos": len(produtos_por_gtin),
            "atualizados": atualizados,
            "recalculados": recalculados,
        }

    def obter_consulta_por_gtin(self, gtin: str) -> dict[str, Any] | None:
        codigo = str(gtin or "").strip()
        if not codigo:
            return None
        with self._managed_conn() as conn:
            row = conn.execute(
                "SELECT * FROM consultas_gtin WHERE gtin = ?",
                (codigo,),
            ).fetchone()
        return dict(row) if row else None

    def obter_ultima_consulta_por_ncm(self, ncm: str) -> dict[str, Any] | None:
        ncm_limpo = "".join(filter(str.isdigit, str(ncm or "")))
        if not ncm_limpo:
            return None
        with self._managed_conn() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM consultas_gtin
                WHERE ncm_winthor = ? OR ncm_oficial = ?
                ORDER BY ultima_atualizacao_ordem DESC, rowid DESC, gtin ASC
                LIMIT 1
                """,
                (ncm_limpo, ncm_limpo),
            ).fetchone()
        return dict(row) if row else None

    def listar_consultas_por_ncm(self, ncm: str, limit: int | None = None) -> list[dict[str, Any]]:
        ncm_limpo = "".join(filter(str.isdigit, str(ncm or "")))
        if not ncm_limpo:
            return []
        sql = """
            SELECT *
            FROM consultas_gtin
            WHERE ncm_winthor = ? OR ncm_oficial = ?
            ORDER BY ultima_atualizacao_ordem DESC, rowid DESC, gtin ASC
        """
        params: list[Any] = [ncm_limpo, ncm_limpo]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(int(limit), 1))
        with self._managed_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def salvar_analise_ia(self, registro: dict[str, Any]) -> int:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        resultado_json = registro.get("resultado_json", {})
        if not isinstance(resultado_json, str):
            resultado_json = json.dumps(resultado_json, ensure_ascii=False)
        payload = {
            "tipo_contexto": str(registro.get("tipo_contexto", "") or "").strip(),
            "chave_contexto": str(registro.get("chave_contexto", "") or "").strip(),
            "contexto_hash": str(registro.get("contexto_hash", "") or "").strip(),
            "origem_contexto": str(registro.get("origem_contexto", "sqlite") or "sqlite"),
            "provider": str(registro.get("provider", "heuristic") or "heuristic"),
            "model": str(registro.get("model", "tax-scenario-heuristic-v1") or "tax-scenario-heuristic-v1"),
            "prompt_version": str(registro.get("prompt_version", "tax-scenario-v1") or "tax-scenario-v1"),
            "status_execucao": str(registro.get("status_execucao", "CONCLUIDA") or "CONCLUIDA"),
            "score_confianca": registro.get("score_confianca"),
            "recomendacao_cst": str(registro.get("recomendacao_cst", "") or ""),
            "recomendacao_cclasstrib": str(registro.get("recomendacao_cclasstrib", "") or ""),
            "requer_revisao_humana": str(registro.get("requer_revisao_humana", "S") or "S"),
            "resumo": str(registro.get("resumo", "") or ""),
            "resultado_json": resultado_json,
            "criado_em": agora,
            "atualizado_em": agora,
        }
        with self._managed_conn() as conn:
            conn.execute(
                """
                INSERT INTO ai_analises (
                    tipo_contexto, chave_contexto, contexto_hash, origem_contexto, provider,
                    model, prompt_version, status_execucao, score_confianca, recomendacao_cst,
                    recomendacao_cclasstrib, requer_revisao_humana, resumo, resultado_json,
                    criado_em, atualizado_em
                ) VALUES (
                    :tipo_contexto, :chave_contexto, :contexto_hash, :origem_contexto, :provider,
                    :model, :prompt_version, :status_execucao, :score_confianca, :recomendacao_cst,
                    :recomendacao_cclasstrib, :requer_revisao_humana, :resumo, :resultado_json,
                    :criado_em, :atualizado_em
                )
                ON CONFLICT(tipo_contexto, chave_contexto, contexto_hash, prompt_version) DO UPDATE SET
                    origem_contexto = excluded.origem_contexto,
                    provider = excluded.provider,
                    model = excluded.model,
                    status_execucao = excluded.status_execucao,
                    score_confianca = excluded.score_confianca,
                    recomendacao_cst = excluded.recomendacao_cst,
                    recomendacao_cclasstrib = excluded.recomendacao_cclasstrib,
                    requer_revisao_humana = excluded.requer_revisao_humana,
                    resumo = excluded.resumo,
                    resultado_json = excluded.resultado_json,
                    atualizado_em = excluded.atualizado_em
                """,
                payload,
            )
            row = conn.execute(
                """
                SELECT id
                FROM ai_analises
                WHERE tipo_contexto = ? AND chave_contexto = ? AND contexto_hash = ? AND prompt_version = ?
                LIMIT 1
                """,
                (
                    payload["tipo_contexto"],
                    payload["chave_contexto"],
                    payload["contexto_hash"],
                    payload["prompt_version"],
                ),
            ).fetchone()
            conn.commit()
        return int(row["id"]) if row else 0

    def obter_analise_ia(self, analise_id: int) -> dict[str, Any] | None:
        with self._managed_conn() as conn:
            row = conn.execute(
                "SELECT * FROM ai_analises WHERE id = ?",
                (int(analise_id),),
            ).fetchone()
        if not row:
            return None
        registro = dict(row)
        try:
            registro["resultado_json"] = json.loads(registro.get("resultado_json") or "{}")
        except json.JSONDecodeError:
            registro["resultado_json"] = {}
        return registro

    def listar_analises_ia(self, filtros: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        filtros = filtros or {}
        sql = "SELECT * FROM ai_analises WHERE 1=1"
        params: list[str] = []
        for campo_sql, campo_filtro in (("tipo_contexto", "tipo_contexto"), ("chave_contexto", "chave_contexto"), ("provider", "provider")):
            valor = str(filtros.get(campo_filtro, "") or "").strip()
            if valor:
                sql += f" AND {campo_sql} LIKE ?"
                params.append(f"%{valor}%")
        sql += " ORDER BY atualizado_em DESC, id DESC"
        with self._managed_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        registros: list[dict[str, Any]] = []
        for row in rows:
            registro = dict(row)
            try:
                registro["resultado_json"] = json.loads(registro.get("resultado_json") or "{}")
            except json.JSONDecodeError:
                registro["resultado_json"] = {}
            registros.append(registro)
        return registros

    def salvar_feedback_analise_ia(
        self,
        analise_id: int,
        decisao: str,
        cst_final: str = "",
        cclasstrib_final: str = "",
        observacao: str = "",
    ) -> int:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with self._managed_conn() as conn:
            existe = conn.execute(
                "SELECT 1 FROM ai_analises WHERE id = ? LIMIT 1",
                (int(analise_id),),
            ).fetchone()
            if not existe:
                raise ValueError(f"Analise IA {analise_id} nao encontrada para registrar feedback.")
            cursor = conn.execute(
                """
                INSERT INTO ai_feedback_analista (
                    analise_id, decisao, cst_final, cclasstrib_final, observacao, criado_em
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(analise_id), decisao, cst_final, cclasstrib_final, observacao, agora),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def listar_feedback_analise_ia(self, analise_id: int) -> list[dict[str, Any]]:
        with self._managed_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM ai_feedback_analista WHERE analise_id = ? ORDER BY id ASC",
                (int(analise_id),),
            ).fetchall()
        return [dict(row) for row in rows]

    def salvar_catalogo_tributario(self, catalogo: list[dict[str, Any]]) -> None:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with self._managed_conn() as conn:
            for item_cst in catalogo:
                conn.execute(
                    """
                    INSERT INTO catalogo_cst_tributario (
                        cst, descricao_cst, ind_ibscbs, ind_red_bc, ind_red_aliq,
                        ind_transf_cred, ind_dif, ind_ajuste_compet, ind_ibscbs_mono,
                        ind_cred_pres_ibs_zfm, publicacao, inicio_vigencia, fim_vigencia,
                        raw_json, ultima_atualizacao
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cst) DO UPDATE SET
                        descricao_cst = excluded.descricao_cst,
                        ind_ibscbs = excluded.ind_ibscbs,
                        ind_red_bc = excluded.ind_red_bc,
                        ind_red_aliq = excluded.ind_red_aliq,
                        ind_transf_cred = excluded.ind_transf_cred,
                        ind_dif = excluded.ind_dif,
                        ind_ajuste_compet = excluded.ind_ajuste_compet,
                        ind_ibscbs_mono = excluded.ind_ibscbs_mono,
                        ind_cred_pres_ibs_zfm = excluded.ind_cred_pres_ibs_zfm,
                        publicacao = excluded.publicacao,
                        inicio_vigencia = excluded.inicio_vigencia,
                        fim_vigencia = excluded.fim_vigencia,
                        raw_json = excluded.raw_json,
                        ultima_atualizacao = excluded.ultima_atualizacao
                    """,
                    (
                        item_cst.get("cst", ""),
                        item_cst.get("descricao_cst", ""),
                        item_cst.get("ind_ibscbs", ""),
                        item_cst.get("ind_red_bc", ""),
                        item_cst.get("ind_red_aliq", ""),
                        item_cst.get("ind_transf_cred", ""),
                        item_cst.get("ind_dif", ""),
                        item_cst.get("ind_ajuste_compet", ""),
                        item_cst.get("ind_ibscbs_mono", ""),
                        item_cst.get("ind_cred_pres_ibs_zfm", ""),
                        item_cst.get("publicacao", ""),
                        item_cst.get("inicio_vigencia", ""),
                        item_cst.get("fim_vigencia", ""),
                        item_cst.get("raw_json", ""),
                        agora,
                    ),
                )
                for classificacao in item_cst.get("classificacoes_tributarias", []):
                    conn.execute(
                        """
                        INSERT INTO dossie_classtrib (
                            cclasstrib, cst, descricao, p_red_ibs, p_red_cbs, tipo_aliquota,
                            ind_trib_regular, ind_cred_pres_oper, ind_estorno_cred,
                            monofasia_sujeita_retencao, monofasia_retida_ant, monofasia_diferimento,
                            monofasia_padrao, ind_nfe, ind_nfce, ind_cte, ind_cteos, ind_bpe,
                            ind_nf3e, ind_nfcom, ind_nfse, ind_bpetm, ind_bpeta, ind_nfag,
                            ind_nfsvia, ind_nfabi, ind_nfgas, ind_dere, anexo, publicacao,
                            inicio_vigencia, fim_vigencia, base_legal, links_legais, raw_json,
                            ultima_atualizacao
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(cclasstrib) DO UPDATE SET
                            cst = excluded.cst,
                            descricao = excluded.descricao,
                            p_red_ibs = excluded.p_red_ibs,
                            p_red_cbs = excluded.p_red_cbs,
                            tipo_aliquota = excluded.tipo_aliquota,
                            ind_trib_regular = excluded.ind_trib_regular,
                            ind_cred_pres_oper = excluded.ind_cred_pres_oper,
                            ind_estorno_cred = excluded.ind_estorno_cred,
                            monofasia_sujeita_retencao = excluded.monofasia_sujeita_retencao,
                            monofasia_retida_ant = excluded.monofasia_retida_ant,
                            monofasia_diferimento = excluded.monofasia_diferimento,
                            monofasia_padrao = excluded.monofasia_padrao,
                            ind_nfe = excluded.ind_nfe,
                            ind_nfce = excluded.ind_nfce,
                            ind_cte = excluded.ind_cte,
                            ind_cteos = excluded.ind_cteos,
                            ind_bpe = excluded.ind_bpe,
                            ind_nf3e = excluded.ind_nf3e,
                            ind_nfcom = excluded.ind_nfcom,
                            ind_nfse = excluded.ind_nfse,
                            ind_bpetm = excluded.ind_bpetm,
                            ind_bpeta = excluded.ind_bpeta,
                            ind_nfag = excluded.ind_nfag,
                            ind_nfsvia = excluded.ind_nfsvia,
                            ind_nfabi = excluded.ind_nfabi,
                            ind_nfgas = excluded.ind_nfgas,
                            ind_dere = excluded.ind_dere,
                            anexo = excluded.anexo,
                            publicacao = excluded.publicacao,
                            inicio_vigencia = excluded.inicio_vigencia,
                            fim_vigencia = excluded.fim_vigencia,
                            base_legal = excluded.base_legal,
                            links_legais = excluded.links_legais,
                            raw_json = excluded.raw_json,
                            ultima_atualizacao = excluded.ultima_atualizacao
                        """,
                        (
                            classificacao.get("cclasstrib", ""),
                            classificacao.get("cst", ""),
                            classificacao.get("descricao", ""),
                            classificacao.get("p_red_ibs", ""),
                            classificacao.get("p_red_cbs", ""),
                            classificacao.get("tipo_aliquota", ""),
                            classificacao.get("ind_trib_regular", ""),
                            classificacao.get("ind_cred_pres_oper", ""),
                            classificacao.get("ind_estorno_cred", ""),
                            classificacao.get("monofasia_sujeita_retencao", ""),
                            classificacao.get("monofasia_retida_ant", ""),
                            classificacao.get("monofasia_diferimento", ""),
                            classificacao.get("monofasia_padrao", ""),
                            classificacao.get("ind_nfe", ""),
                            classificacao.get("ind_nfce", ""),
                            classificacao.get("ind_cte", ""),
                            classificacao.get("ind_cteos", ""),
                            classificacao.get("ind_bpe", ""),
                            classificacao.get("ind_nf3e", ""),
                            classificacao.get("ind_nfcom", ""),
                            classificacao.get("ind_nfse", ""),
                            classificacao.get("ind_bpetm", ""),
                            classificacao.get("ind_bpeta", ""),
                            classificacao.get("ind_nfag", ""),
                            classificacao.get("ind_nfsvia", ""),
                            classificacao.get("ind_nfabi", ""),
                            classificacao.get("ind_nfgas", ""),
                            classificacao.get("ind_dere", ""),
                            classificacao.get("anexo", ""),
                            classificacao.get("publicacao", ""),
                            classificacao.get("inicio_vigencia", ""),
                            classificacao.get("fim_vigencia", ""),
                            classificacao.get("base_legal", ""),
                            json.dumps(classificacao.get("links_legais", []), ensure_ascii=False),
                            classificacao.get("raw_json", ""),
                            agora,
                        ),
                    )
            conn.commit()

    def obter_dossie_cache(self, cclasstrib: str) -> dict[str, Any] | None:
        with self._managed_conn() as conn:
            row = conn.execute(
                "SELECT * FROM dossie_classtrib WHERE cclasstrib = ?",
                (str(cclasstrib).strip(),),
            ).fetchone()
        if not row:
            return None
        registro = dict(row)
        links_raw = registro.get("links_legais") or "[]"
        try:
            registro["links_legais"] = json.loads(links_raw)
        except json.JSONDecodeError:
            registro["links_legais"] = []
        return registro

    def listar_catalogo_cst(self) -> list[dict[str, Any]]:
        with self._managed_conn() as conn:
            rows = conn.execute("SELECT * FROM catalogo_cst_tributario ORDER BY cst ASC").fetchall()
        return [dict(row) for row in rows]

    def salvar_anexos_tributarios(self, anexos: list[dict[str, Any]], substituir: bool = False) -> None:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with self._managed_conn() as conn:
            if substituir and anexos:
                conn.execute("DELETE FROM anexos_especificidades")
                conn.execute("DELETE FROM anexos_tributarios")
            for anexo in anexos:
                codigo_anexo = str(anexo.get("anexo", "") or "").strip()
                if not codigo_anexo:
                    continue
                conn.execute(
                    """
                    INSERT INTO anexos_tributarios (
                        anexo, descricao, publicacao, inicio_vigencia, fim_vigencia,
                        raw_json, ultima_atualizacao
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(anexo) DO UPDATE SET
                        descricao = excluded.descricao,
                        publicacao = excluded.publicacao,
                        inicio_vigencia = excluded.inicio_vigencia,
                        fim_vigencia = excluded.fim_vigencia,
                        raw_json = excluded.raw_json,
                        ultima_atualizacao = excluded.ultima_atualizacao
                    """,
                    (
                        codigo_anexo,
                        anexo.get("descricao", ""),
                        anexo.get("publicacao", ""),
                        anexo.get("inicio_vigencia", ""),
                        anexo.get("fim_vigencia", ""),
                        anexo.get("raw_json", ""),
                        agora,
                    ),
                )
                conn.execute("DELETE FROM anexos_especificidades WHERE anexo = ?", (codigo_anexo,))
                for especificidade in anexo.get("especificidades", []):
                    conn.execute(
                        """
                        INSERT INTO anexos_especificidades (
                            anexo, codigo, descricao, valor, tipo, publicacao,
                            inicio_vigencia, fim_vigencia, raw_json, ultima_atualizacao
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(anexo, codigo, descricao, valor) DO UPDATE SET
                            tipo = excluded.tipo,
                            publicacao = excluded.publicacao,
                            inicio_vigencia = excluded.inicio_vigencia,
                            fim_vigencia = excluded.fim_vigencia,
                            raw_json = excluded.raw_json,
                            ultima_atualizacao = excluded.ultima_atualizacao
                        """,
                        (
                            codigo_anexo,
                            especificidade.get("codigo", ""),
                            especificidade.get("descricao", ""),
                            especificidade.get("valor", ""),
                            especificidade.get("tipo", ""),
                            especificidade.get("publicacao", ""),
                            especificidade.get("inicio_vigencia", ""),
                            especificidade.get("fim_vigencia", ""),
                            especificidade.get("raw_json", ""),
                            agora,
                        ),
                    )
            conn.commit()

    def obter_anexo_cache(self, codigo_anexo: str) -> dict[str, Any] | None:
        codigo = str(codigo_anexo or "").strip()
        if not codigo:
            return None
        with self._managed_conn() as conn:
            anexo_row = conn.execute(
                "SELECT * FROM anexos_tributarios WHERE anexo = ?",
                (codigo,),
            ).fetchone()
            if not anexo_row:
                return None
            especificidades_rows = conn.execute(
                "SELECT codigo, descricao, valor, tipo, publicacao, inicio_vigencia, fim_vigencia, raw_json FROM anexos_especificidades WHERE anexo = ? ORDER BY codigo ASC, descricao ASC",
                (codigo,),
            ).fetchall()
        registro = dict(anexo_row)
        registro["especificidades"] = [dict(row) for row in especificidades_rows]
        return registro

    def listar_anexos_tributarios(self) -> list[dict[str, Any]]:
        with self._managed_conn() as conn:
            rows = conn.execute("SELECT * FROM anexos_tributarios ORDER BY anexo ASC").fetchall()
        return [dict(row) for row in rows]

    def listar_retorno_anexos(self, filtros: dict | None = None) -> list[dict[str, Any]]:
        filtros = filtros or {}
        sql = """
            WITH referencias_base AS (
                SELECT
                    c.ncm,
                    c.cst,
                    c.cclasstrib,
                    d.anexo
                FROM cenarios_ncm_base c
                JOIN dossie_classtrib d ON d.cclasstrib = c.cclasstrib
                WHERE TRIM(COALESCE(d.anexo, '')) <> ''
            ),
            totais_por_anexo AS (
                SELECT
                    anexo,
                    COUNT(*) AS total_cenarios_relacionados,
                    COUNT(DISTINCT ncm) AS total_ncms_relacionados
                FROM referencias_base
                GROUP BY anexo
            ),
            ncms_por_anexo AS (
                SELECT anexo, GROUP_CONCAT(ncm) AS ncms_relacionados
                FROM (
                    SELECT DISTINCT anexo, ncm
                    FROM referencias_base
                    ORDER BY anexo, ncm
                ) refs
                GROUP BY anexo
            ),
            csts_por_anexo AS (
                SELECT anexo, GROUP_CONCAT(cst) AS csts_relacionados
                FROM (
                    SELECT DISTINCT anexo, cst
                    FROM referencias_base
                    WHERE TRIM(COALESCE(cst, '')) <> ''
                    ORDER BY anexo, cst
                ) refs
                GROUP BY anexo
            ),
            cclasstrib_por_anexo AS (
                SELECT anexo, GROUP_CONCAT(cclasstrib) AS cclasstrib_relacionados
                FROM (
                    SELECT DISTINCT anexo, cclasstrib
                    FROM referencias_base
                    WHERE TRIM(COALESCE(cclasstrib, '')) <> ''
                    ORDER BY anexo, cclasstrib
                ) refs
                GROUP BY anexo
            )
            SELECT
                a.anexo,
                a.descricao,
                a.publicacao,
                a.inicio_vigencia,
                a.fim_vigencia,
                a.ultima_atualizacao,
                COALESCE(t.total_ncms_relacionados, 0) AS total_ncms_relacionados,
                COALESCE(t.total_cenarios_relacionados, 0) AS total_cenarios_relacionados,
                COALESCE(n.ncms_relacionados, '') AS ncms_relacionados,
                COALESCE(cs.csts_relacionados, '') AS csts_relacionados,
                COALESCE(cc.cclasstrib_relacionados, '') AS cclasstrib_relacionados,
                e.codigo AS codigo_especificidade,
                e.descricao AS descricao_especificidade,
                e.valor,
                e.tipo,
                e.publicacao AS especificidade_publicacao,
                e.inicio_vigencia AS especificidade_inicio_vigencia,
                e.fim_vigencia AS especificidade_fim_vigencia
            FROM anexos_tributarios a
            LEFT JOIN totais_por_anexo t ON t.anexo = a.anexo
            LEFT JOIN ncms_por_anexo n ON n.anexo = a.anexo
            LEFT JOIN csts_por_anexo cs ON cs.anexo = a.anexo
            LEFT JOIN cclasstrib_por_anexo cc ON cc.anexo = a.anexo
            LEFT JOIN anexos_especificidades e ON e.anexo = a.anexo
            WHERE 1=1
        """
        params: list[str] = []
        for campo_sql, campo_filtro in (
            ("a.anexo", "anexo"),
            ("a.descricao", "descricao"),
            ("e.codigo", "codigo_especificidade"),
            ("e.descricao", "descricao_especificidade"),
            ("e.tipo", "tipo"),
        ):
            valor = (filtros.get(campo_filtro) or "").strip()
            if valor:
                sql += f" AND {campo_sql} LIKE ?"
                params.append(f"%{valor}%")
        sql += " ORDER BY a.anexo ASC, e.codigo ASC, e.descricao ASC"
        with self._managed_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def _garantir_dossie_placeholder(
        self,
        conn: sqlite3.Connection,
        cclasstrib: str,
        cst: str = "",
        descricao: str = "",
        ultima_atualizacao: str | None = None,
    ) -> None:
        codigo = str(cclasstrib or "").strip()
        if not codigo:
            return
        conn.execute(
            """
            INSERT INTO dossie_classtrib (
                cclasstrib, cst, descricao, raw_json, ultima_atualizacao
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cclasstrib) DO UPDATE SET
                cst = CASE
                    WHEN TRIM(COALESCE(dossie_classtrib.cst, '')) = '' THEN excluded.cst
                    ELSE dossie_classtrib.cst
                END,
                descricao = CASE
                    WHEN TRIM(COALESCE(dossie_classtrib.descricao, '')) = '' THEN excluded.descricao
                    ELSE dossie_classtrib.descricao
                END
            """,
            (
                codigo,
                str(cst or "").strip(),
                str(descricao or "").strip(),
                "{}",
                ultima_atualizacao or datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            ),
        )

    def salvar_cenarios_tributarios(self, ncm: str, cenarios: list[dict[str, Any]]) -> None:
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        ncm_limpo = "".join(filter(str.isdigit, str(ncm or "")))
        cenarios_unicos: list[dict[str, Any]] = []
        chaves_vistas: set[tuple[str, str, str, str]] = set()
        for cenario in cenarios:
            chave = (
                ncm_limpo,
                str(cenario.get("cclasstrib", "") or "").strip(),
                str(cenario.get("cst", "") or "").strip(),
                str(cenario.get("condicao_legal", "") or "").strip(),
            )
            if chave in chaves_vistas:
                continue
            chaves_vistas.add(chave)
            cenarios_unicos.append(cenario)

        with self._managed_conn() as conn:
            conn.execute("DELETE FROM cenarios_ncm_base WHERE ncm = ?", (ncm_limpo,))
            for cenario in cenarios_unicos:
                conn.execute(
                    """
                    INSERT INTO cenarios_ncm_base (
                        ncm, cst, cclasstrib, condicao_legal, fonte, ultima_atualizacao
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ncm_limpo,
                        str(cenario.get("cst", "") or "").strip(),
                        str(cenario.get("cclasstrib", "") or "").strip(),
                        str(cenario.get("condicao_legal", "") or "").strip(),
                        str(cenario.get("fonte", "") or "").strip(),
                        agora,
                    ),
                )
            conn.commit()

    def listar_consultas(self, filtros: dict | None = None) -> list[dict[str, Any]]:
        filtros = filtros or {}
        sql = "SELECT * FROM consultas_gtin WHERE 1=1"
        params: list[str] = []
        cod_winthor = (filtros.get("cod_winthor") or "").strip()
        gtin = (filtros.get("gtin") or "").strip()
        status = (filtros.get("status_sefaz") or "").strip()
        divergencia = (filtros.get("divergencia_ncm") or "").strip()
        ncm = (filtros.get("ncm") or "").strip()
        descricao = (filtros.get("descricao_produto") or "").strip()
        if cod_winthor:
            sql += " AND cod_winthor LIKE ?"
            params.append(f"%{cod_winthor}%")
        if gtin:
            sql += " AND gtin LIKE ?"
            params.append(f"%{gtin}%")
        if status:
            sql += " AND status_sefaz LIKE ?"
            params.append(f"%{status}%")
        if divergencia:
            sql += " AND divergencia_ncm LIKE ?"
            params.append(f"%{divergencia}%")
        if ncm:
            sql += " AND (ncm_winthor LIKE ? OR ncm_oficial LIKE ?)"
            params.extend([f"%{ncm}%", f"%{ncm}%"])
        if descricao:
            sql += " AND (descricao_produto LIKE ? OR descricao_erp LIKE ?)"
            params.extend([f"%{descricao}%", f"%{descricao}%"])
        sql += " ORDER BY ultima_atualizacao_ordem DESC, rowid DESC, gtin ASC"
        with self._managed_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def listar_cenarios_tributarios(self, filtros: dict | None = None) -> list[dict[str, Any]]:
        filtros = filtros or {}
        sql = """
            SELECT
                c.id,
                c.ncm,
                c.cst,
                c.cclasstrib,
                c.condicao_legal,
                COALESCE(d.descricao, '') AS descricao_dossie,
                COALESCE(d.p_red_ibs, '') AS p_red_ibs,
                COALESCE(d.p_red_cbs, '') AS p_red_cbs,
                COALESCE(d.publicacao, '') AS publicacao,
                COALESCE(d.inicio_vigencia, '') AS inicio_vigencia,
                COALESCE(d.anexo, '') AS anexo,
                COALESCE(d.ind_nfe, '') AS ind_nfe,
                COALESCE(d.ind_nfce, '') AS ind_nfce,
                COALESCE(d.base_legal, '') AS base_legal,
                c.fonte,
                c.ultima_atualizacao
            FROM cenarios_ncm_base c
            LEFT JOIN dossie_classtrib d ON d.cclasstrib = c.cclasstrib
            WHERE 1=1
        """
        params: list[str] = []
        for campo_sql, campo_filtro in (
            ("c.ncm", "ncm"),
            ("c.cst", "cst"),
            ("c.cclasstrib", "cclasstrib"),
            ("c.condicao_legal", "condicao_legal"),
        ):
            valor = (filtros.get(campo_filtro) or "").strip()
            if valor:
                sql += f" AND {campo_sql} LIKE ?"
                params.append(f"%{valor}%")

        descricao = (filtros.get("descricao_dossie") or "").strip()
        if descricao:
            sql += " AND (COALESCE(d.descricao, '') LIKE ? OR COALESCE(c.condicao_legal, '') LIKE ? OR COALESCE(d.base_legal, '') LIKE ?)"
            params.extend([f"%{descricao}%", f"%{descricao}%", f"%{descricao}%"])

        sql += " ORDER BY c.ultima_atualizacao DESC, c.ncm ASC, c.cst ASC, c.cclasstrib ASC"
        with self._managed_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def buscar_status_por_gtins(self, gtins: list[str], chunk_size: int = 900) -> dict[str, dict[str, str]]:
        gtins_limpos = [str(gtin).strip() for gtin in gtins if str(gtin).strip()]
        if not gtins_limpos:
            return {}
        retorno: dict[str, dict[str, str]] = {}
        with self._managed_conn() as conn:
            for inicio in range(0, len(gtins_limpos), chunk_size):
                lote = gtins_limpos[inicio : inicio + chunk_size]
                placeholders = ",".join("?" for _ in lote)
                sql = (
                    "SELECT gtin, status_sefaz, divergencia_ncm, ultima_atualizacao "
                    f"FROM consultas_gtin WHERE gtin IN ({placeholders})"
                )
                rows = conn.execute(sql, lote).fetchall()
                for row in rows:
                    retorno[str(row["gtin"])] = {
                        "status_sefaz": row["status_sefaz"] or "",
                        "divergencia_ncm": row["divergencia_ncm"] or "",
                        "ultima_atualizacao": row["ultima_atualizacao"] or "",
                    }
        return retorno

    def contar_consultas(self) -> int:
        with self._managed_conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM consultas_gtin").fetchone()
        return int(row["total"]) if row else 0

    def obter_resumo_estatistico(self) -> dict[str, int]:
        with self._managed_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM consultas_gtin").fetchone()[0]
            ok = conn.execute(
                "SELECT COUNT(*) FROM consultas_gtin WHERE divergencia_ncm = 'OK'"
            ).fetchone()[0]
            divergentes = conn.execute(
                "SELECT COUNT(*) FROM consultas_gtin WHERE divergencia_ncm LIKE 'DIVERGENTE%'"
            ).fetchone()[0]
            outros = total - (ok + divergentes)
            total_cenarios = conn.execute("SELECT COUNT(*) FROM cenarios_ncm_base").fetchone()[0]
            total_dossies = conn.execute("SELECT COUNT(*) FROM dossie_classtrib").fetchone()[0]
            total_anexos = conn.execute("SELECT COUNT(*) FROM anexos_tributarios").fetchone()[0]
        return {
            "total": total,
            "ok": ok,
            "divergentes": divergentes,
            "outros": outros,
            "total_cenarios": total_cenarios,
            "total_dossies": total_dossies,
            "total_anexos": total_anexos,
        }
