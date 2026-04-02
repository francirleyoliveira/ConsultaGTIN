from __future__ import annotations

import hashlib
import json
from typing import Any

from app.services.sqlite_service import ConsultaGtinRepository


class AIContextService:
    def __init__(self, repository: ConsultaGtinRepository) -> None:
        self.repository = repository

    def montar_contexto_por_gtin(self, gtin: str) -> dict[str, Any]:
        consulta = self.repository.obter_consulta_por_gtin(gtin)
        if not consulta:
            raise ValueError(f"GTIN {gtin} nao encontrado na base local.")
        ncm = consulta.get("ncm_winthor") or consulta.get("ncm_oficial") or ""
        return self._montar_contexto("GTIN", str(gtin).strip(), ncm, [consulta])

    def montar_contexto_por_ncm(self, ncm: str) -> dict[str, Any]:
        ncm_limpo = "".join(filter(str.isdigit, str(ncm or "")))
        if not ncm_limpo:
            raise ValueError("NCM invalido para analise de IA.")
        consultas = self.repository.listar_consultas_por_ncm(ncm_limpo, limit=20)
        return self._montar_contexto("NCM", ncm_limpo, ncm_limpo, consultas)

    def _montar_contexto(
        self,
        tipo_contexto: str,
        chave_contexto: str,
        ncm: str,
        consultas_contexto: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        consultas_contexto = list(consultas_contexto or [])
        consulta_referencia = consultas_contexto[0] if consultas_contexto else {}
        cenarios_brutos = self.repository.listar_cenarios_tributarios({"ncm": ncm}) if ncm else []
        cenarios: list[dict[str, Any]] = []
        anexos_considerados: dict[str, dict[str, Any]] = {}

        for cenario in cenarios_brutos:
            codigo_anexo = str(cenario.get("anexo", "") or "").strip()
            anexo = self.repository.obter_anexo_cache(codigo_anexo) if codigo_anexo else None
            particularidades = [
                {
                    "codigo": str(item.get("codigo", "") or ""),
                    "descricao": str(item.get("descricao", "") or ""),
                    "valor": str(item.get("valor", "") or ""),
                    "tipo": str(item.get("tipo", "") or ""),
                    "publicacao": str(item.get("publicacao", "") or ""),
                    "inicio_vigencia": str(item.get("inicio_vigencia", "") or ""),
                    "fim_vigencia": str(item.get("fim_vigencia", "") or ""),
                }
                for item in (anexo or {}).get("especificidades", [])
            ]

            cenarios.append(
                {
                    "ncm": str(cenario.get("ncm", "") or ""),
                    "cst": str(cenario.get("cst", "") or ""),
                    "cclasstrib": str(cenario.get("cclasstrib", "") or ""),
                    "condicao_legal": str(cenario.get("condicao_legal", "") or ""),
                    "descricao_dossie": str(cenario.get("descricao_dossie", "") or ""),
                    "p_red_ibs": str(cenario.get("p_red_ibs", "") or ""),
                    "p_red_cbs": str(cenario.get("p_red_cbs", "") or ""),
                    "publicacao": str(cenario.get("publicacao", "") or ""),
                    "inicio_vigencia": str(cenario.get("inicio_vigencia", "") or ""),
                    "anexo": codigo_anexo,
                    "ind_nfe": str(cenario.get("ind_nfe", "") or ""),
                    "ind_nfce": str(cenario.get("ind_nfce", "") or ""),
                    "base_legal": str(cenario.get("base_legal", "") or ""),
                    "fonte": str(cenario.get("fonte", "") or ""),
                    "anexo_detalhe": {
                        "anexo": codigo_anexo,
                        "descricao": str((anexo or {}).get("descricao", "") or ""),
                        "publicacao": str((anexo or {}).get("publicacao", "") or ""),
                        "inicio_vigencia": str((anexo or {}).get("inicio_vigencia", "") or ""),
                        "fim_vigencia": str((anexo or {}).get("fim_vigencia", "") or ""),
                    } if anexo else None,
                    "particularidades_anexo": particularidades,
                }
            )

            if codigo_anexo and codigo_anexo not in anexos_considerados:
                anexos_considerados[codigo_anexo] = {
                    "anexo": codigo_anexo,
                    "descricao": str((anexo or {}).get("descricao", "") or ""),
                    "publicacao": str((anexo or {}).get("publicacao", "") or ""),
                    "inicio_vigencia": str((anexo or {}).get("inicio_vigencia", "") or ""),
                    "fim_vigencia": str((anexo or {}).get("fim_vigencia", "") or ""),
                    "quantidade_particularidades": len(particularidades),
                    "particularidades": particularidades,
                }

        produto = self._montar_produto_contexto(tipo_contexto, ncm, consulta_referencia, consultas_contexto)
        contexto = {
            "tipo_contexto": tipo_contexto,
            "chave_contexto": chave_contexto,
            "produto": produto,
            "ncm_referencia": ncm,
            "cenarios": cenarios,
            "anexos_considerados": list(anexos_considerados.values()),
            "consultas_relacionadas": [
                {
                    "cod_winthor": str(item.get("cod_winthor", "") or ""),
                    "gtin": str(item.get("gtin", "") or ""),
                    "ncm_erp": str(item.get("ncm_winthor", "") or ""),
                    "ncm_gs1": str(item.get("ncm_oficial", "") or ""),
                    "descricao_erp": str(item.get("descricao_erp", "") or ""),
                    "descricao_gs1": str(item.get("descricao_produto", "") or ""),
                    "ultima_atualizacao": str(item.get("ultima_atualizacao", "") or ""),
                }
                for item in consultas_contexto[:10]
            ],
            "resumo_operacional": {
                "total_cenarios": len(cenarios),
                "total_anexos": len(anexos_considerados),
                "possui_divergencia": str(produto.get("divergencia_ncm", "")).startswith("DIVERGENTE"),
                "consulta_gtin_disponivel": bool(produto.get("gtin")),
                "contexto_agregado": bool(produto.get("contexto_agregado")),
                "quantidade_produtos_contexto": int(produto.get("quantidade_produtos_contexto", 0) or 0),
            },
        }
        contexto["contexto_hash"] = self._gerar_hash(contexto)
        return contexto

    def _montar_produto_contexto(
        self,
        tipo_contexto: str,
        ncm: str,
        consulta_referencia: dict[str, Any],
        consultas_contexto: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if tipo_contexto == "GTIN":
            return {
                "cod_winthor": str((consulta_referencia or {}).get("cod_winthor", "") or ""),
                "gtin": str((consulta_referencia or {}).get("gtin", "") or ""),
                "ncm_erp": str((consulta_referencia or {}).get("ncm_winthor", "") or ""),
                "ncm_gs1": str((consulta_referencia or {}).get("ncm_oficial", "") or ""),
                "descricao_erp": str((consulta_referencia or {}).get("descricao_erp", "") or ""),
                "descricao_gs1": str((consulta_referencia or {}).get("descricao_produto", "") or ""),
                "divergencia_ncm": str((consulta_referencia or {}).get("divergencia_ncm", "") or ""),
                "status_sefaz": str((consulta_referencia or {}).get("status_sefaz", "") or ""),
                "motivo_sefaz": str((consulta_referencia or {}).get("motivo_sefaz", "") or ""),
                "cest": str((consulta_referencia or {}).get("cest", "") or ""),
                "contexto_agregado": False,
                "quantidade_produtos_contexto": 1 if consulta_referencia else 0,
                "amostra_gtins": [str((consulta_referencia or {}).get("gtin", "") or "")] if consulta_referencia else [],
                "amostra_descricoes_erp": [str((consulta_referencia or {}).get("descricao_erp", "") or "")] if consulta_referencia else [],
            }

        gtins = []
        codigos = []
        descricoes_erp = []
        descricoes_gs1 = []
        ncms_gs1 = []
        divergencias = []
        for consulta in consultas_contexto:
            gtin = str(consulta.get("gtin", "") or "").strip()
            cod = str(consulta.get("cod_winthor", "") or "").strip()
            desc_erp = str(consulta.get("descricao_erp", "") or "").strip()
            desc_gs1 = str(consulta.get("descricao_produto", "") or "").strip()
            ncm_gs1 = str(consulta.get("ncm_oficial", "") or "").strip()
            divergencia = str(consulta.get("divergencia_ncm", "") or "").strip()
            if gtin and gtin not in gtins:
                gtins.append(gtin)
            if cod and cod not in codigos:
                codigos.append(cod)
            if desc_erp and desc_erp not in descricoes_erp:
                descricoes_erp.append(desc_erp)
            if desc_gs1 and desc_gs1 not in descricoes_gs1:
                descricoes_gs1.append(desc_gs1)
            if ncm_gs1 and ncm_gs1 not in ncms_gs1:
                ncms_gs1.append(ncm_gs1)
            if divergencia and divergencia not in divergencias:
                divergencias.append(divergencia)

        quantidade_produtos = len(gtins) or len(consultas_contexto)
        descricao_erp = " | ".join(descricoes_erp[:5])
        descricao_gs1 = " | ".join(descricoes_gs1[:5])
        return {
            "cod_winthor": codigos[0] if len(codigos) == 1 else "MULTIPLOS",
            "gtin": gtins[0] if len(gtins) == 1 else "",
            "ncm_erp": ncm,
            "ncm_gs1": ncms_gs1[0] if len(ncms_gs1) == 1 else "",
            "descricao_erp": descricao_erp,
            "descricao_gs1": descricao_gs1,
            "divergencia_ncm": divergencias[0] if len(divergencias) == 1 else "",
            "status_sefaz": "AGREGADO_NCM",
            "motivo_sefaz": f"Analise agregada por NCM com {quantidade_produtos} produto(s) no contexto local.",
            "cest": str((consulta_referencia or {}).get("cest", "") or ""),
            "contexto_agregado": True,
            "quantidade_produtos_contexto": quantidade_produtos,
            "amostra_gtins": gtins[:10],
            "amostra_descricoes_erp": descricoes_erp[:10],
            "amostra_descricoes_gs1": descricoes_gs1[:10],
        }

    def _gerar_hash(self, contexto: dict[str, Any]) -> str:
        serializado = json.dumps(contexto, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serializado.encode("utf-8")).hexdigest()
