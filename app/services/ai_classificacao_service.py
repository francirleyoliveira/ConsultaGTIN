from __future__ import annotations

import re
from typing import Any, Protocol

from app.config import Settings
from app.services.ai_context_service import AIContextService
from app.services.sqlite_service import ConsultaGtinRepository


class AIClassificationProvider(Protocol):
    provider_name: str
    model_name: str

    def analisar(self, contexto: dict[str, Any], prompt_version: str) -> dict[str, Any]: ...


class HeuristicAIClassificationProvider:
    provider_name = "heuristic"
    model_name = "tax-scenario-heuristic-v1"

    def analisar(self, contexto: dict[str, Any], prompt_version: str) -> dict[str, Any]:
        produto = contexto.get("produto", {})
        cenarios = contexto.get("cenarios", [])
        anexos = contexto.get("anexos_considerados", [])

        inconsistencias: list[str] = []
        divergencia = str(produto.get("divergencia_ncm", "") or "")
        if divergencia.startswith("DIVERGENTE"):
            inconsistencias.append(divergencia)

        status_sefaz = str(produto.get("status_sefaz", "") or "")
        if status_sefaz and status_sefaz not in {"949", "9490", "AGREGADO_NCM"}:
            inconsistencias.append(f"Consulta GTIN com status {status_sefaz}: {produto.get('motivo_sefaz', '')}".strip())

        if produto.get("contexto_agregado"):
            inconsistencias.append(
                "Analise por NCM baseada em contexto agregado de multiplos produtos; prefira selecionar um GTIN para recomendacao item-especifica."
            )

        dados_faltantes: list[str] = []
        if not produto.get("descricao_erp"):
            dados_faltantes.append("Descricao ERP ausente para confronto com cenarios e particularidades dos anexos.")
        if not produto.get("descricao_gs1") and not produto.get("contexto_agregado"):
            dados_faltantes.append("Descricao GS1 ausente ou nao consultada na ultima carga.")
        if not produto.get("ncm_gs1") and not produto.get("contexto_agregado"):
            dados_faltantes.append("NCM GS1 nao disponivel no cache local.")
        if produto.get("contexto_agregado"):
            dados_faltantes.append("A analise por NCM usa amostras de produtos do ERP; valide o item final antes de decidir CST/cClassTrib.")
        if not cenarios:
            dados_faltantes.append("Nao ha cenarios tributarios persistidos para o NCM informado.")

        alternativas = [self._avaliar_cenario(produto, cenario) for cenario in cenarios]
        alternativas.sort(key=lambda item: item["score"], reverse=True)
        recomendacao = alternativas[0] if alternativas else None

        score_confianca = float(recomendacao["score"]) if recomendacao else 0.0
        resumo = self._montar_resumo(produto, alternativas, anexos, inconsistencias)

        return {
            "provider": self.provider_name,
            "model": self.model_name,
            "prompt_version": prompt_version,
            "status_execucao": "CONCLUIDA",
            "score_confianca": score_confianca,
            "resumo": resumo,
            "cenario_recomendado": recomendacao,
            "alternativas": alternativas,
            "inconsistencias": inconsistencias,
            "dados_faltantes": dados_faltantes,
            "anexos_considerados": anexos,
            "necessita_validacao_humana": True,
            "requer_revisao_humana": "S",
        }

    def _avaliar_cenario(self, produto: dict[str, Any], cenario: dict[str, Any]) -> dict[str, Any]:
        texto_produto = " ".join(
            parte for parte in [
                str(produto.get("descricao_erp", "") or ""),
                str(produto.get("descricao_gs1", "") or ""),
            ] if parte
        )
        tokens_produto = self._tokenizar(texto_produto)
        tokens_cenario = self._tokenizar(
            " ".join(
                parte for parte in [
                    str(cenario.get("descricao_dossie", "") or ""),
                    str(cenario.get("condicao_legal", "") or ""),
                    str((cenario.get("anexo_detalhe") or {}).get("descricao", "") or ""),
                ] if parte
            )
        )

        intersecao_cenario = tokens_produto & tokens_cenario
        score = 0.12 if len(tokens_produto) == 0 else 0.25
        if intersecao_cenario:
            score += min(0.35, 0.08 * len(intersecao_cenario))

        especificidades_relevantes: list[dict[str, Any]] = []
        for especificidade in cenario.get("particularidades_anexo", []):
            tokens_especificidade = self._tokenizar(
                " ".join(
                    parte for parte in [
                        str(especificidade.get("descricao", "") or ""),
                        str(especificidade.get("valor", "") or ""),
                    ] if parte
                )
            )
            intersecao = tokens_produto & tokens_especificidade
            if intersecao:
                score += min(0.25, 0.06 * len(intersecao))
                especificidades_relevantes.append(
                    {
                        "codigo": especificidade.get("codigo", ""),
                        "descricao": especificidade.get("descricao", ""),
                        "valor": especificidade.get("valor", ""),
                        "tipo": especificidade.get("tipo", ""),
                        "tokens_em_comum": sorted(intersecao),
                    }
                )

        anexo = cenario.get("anexo_detalhe") or {}
        restricoes_do_anexo: list[str] = []
        if anexo.get("anexo"):
            restricoes_do_anexo.append(
                f"Anexo {anexo.get('anexo')} considerado: {anexo.get('descricao', '')}".strip()
            )
            score += 0.1
            if cenario.get("particularidades_anexo") and not especificidades_relevantes:
                restricoes_do_anexo.append(
                    "Existem particularidades no anexo que nao foram claramente relacionadas ao texto do produto."
                )
                score -= 0.05

        if produto.get("contexto_agregado"):
            restricoes_do_anexo.append("Contexto agregado por NCM: a aderencia precisa de validacao humana no item final.")
            score -= 0.08

        motivos_favoraveis: list[str] = []
        if intersecao_cenario:
            motivos_favoraveis.append(
                "Correspondencia textual entre o produto e a descricao/condicao do cenario: "
                + ", ".join(sorted(intersecao_cenario))
            )
        if especificidades_relevantes:
            motivos_favoraveis.append(
                f"{len(especificidades_relevantes)} particularidade(s) de anexo aderente(s) ao produto."
            )
        if not motivos_favoraveis:
            motivos_favoraveis.append(
                "Cenario oficial considerado, mas com evidencias textuais limitadas; revisar com apoio humano."
            )

        score = max(0.0, min(round(score, 3), 0.99))
        return {
            "cst": str(cenario.get("cst", "") or ""),
            "cclasstrib": str(cenario.get("cclasstrib", "") or ""),
            "anexo": str(cenario.get("anexo", "") or ""),
            "condicao_legal": str(cenario.get("condicao_legal", "") or ""),
            "descricao_dossie": str(cenario.get("descricao_dossie", "") or ""),
            "score": score,
            "motivos_favoraveis": motivos_favoraveis,
            "restricoes_do_anexo": restricoes_do_anexo,
            "especificidades_relevantes": especificidades_relevantes,
        }

    def _montar_resumo(
        self,
        produto: dict[str, Any],
        alternativas: list[dict[str, Any]],
        anexos: list[dict[str, Any]],
        inconsistencias: list[str],
    ) -> str:
        if not alternativas:
            return "Nao ha cenarios suficientes para recomendacao; a IA depende dos cenarios oficiais persistidos e dos anexos relacionados."
        melhor = alternativas[0]
        partes = [
            f"Foram avaliados {len(alternativas)} cenario(s) oficial(is) para o NCM {produto.get('ncm_erp') or produto.get('ncm_gs1') or ''}.",
            f"O cenario com maior aderencia inicial foi CST {melhor.get('cst')} / cClassTrib {melhor.get('cclasstrib')}.",
            f"A analise considerou {len(anexos)} anexo(s) e suas particularidades persistidas.",
        ]
        if produto.get("contexto_agregado"):
            partes.append(
                f"O contexto foi agregado a partir de {produto.get('quantidade_produtos_contexto', 0)} produto(s), entao a recomendacao nao deve ser tratada como item-especifica sem revisao humana."
            )
        if inconsistencias:
            partes.append("Ha inconsistencia(s) operacional(is) que exigem revisao humana antes de qualquer decisao.")
        return " ".join(parte for parte in partes if parte).strip()

    def _tokenizar(self, texto: str) -> set[str]:
        return {
            token.lower()
            for token in re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9]+", texto or "")
            if len(token) >= 3
        }


class AIClassificationService:
    def __init__(
        self,
        settings: Settings,
        repository: ConsultaGtinRepository,
        context_service: AIContextService | None = None,
        provider: AIClassificationProvider | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.context_service = context_service or AIContextService(repository)
        self.provider = provider or HeuristicAIClassificationProvider()

    def analisar_gtin(self, gtin: str) -> dict[str, Any]:
        contexto = self.context_service.montar_contexto_por_gtin(gtin)
        return self._analisar_contexto(contexto)

    def analisar_ncm(self, ncm: str) -> dict[str, Any]:
        contexto = self.context_service.montar_contexto_por_ncm(ncm)
        return self._analisar_contexto(contexto)

    def registrar_feedback(
        self,
        analise_id: int,
        decisao: str,
        cst_final: str = "",
        cclasstrib_final: str = "",
        observacao: str = "",
    ) -> int:
        return self.repository.salvar_feedback_analise_ia(
            analise_id=analise_id,
            decisao=decisao,
            cst_final=cst_final,
            cclasstrib_final=cclasstrib_final,
            observacao=observacao,
        )

    def _analisar_contexto(self, contexto: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.ai_enabled:
            raise RuntimeError("Integracao de IA desabilitada na configuracao atual.")

        resultado = self.provider.analisar(contexto, self.settings.ai_prompt_version)
        recomendacao = resultado.get("cenario_recomendado") or {}
        analise_id = self.repository.salvar_analise_ia(
            {
                "tipo_contexto": contexto.get("tipo_contexto", ""),
                "chave_contexto": contexto.get("chave_contexto", ""),
                "contexto_hash": contexto.get("contexto_hash", ""),
                "origem_contexto": "sqlite",
                "provider": resultado.get("provider", self.provider.provider_name),
                "model": resultado.get("model", self.provider.model_name),
                "prompt_version": resultado.get("prompt_version", self.settings.ai_prompt_version),
                "status_execucao": resultado.get("status_execucao", "CONCLUIDA"),
                "score_confianca": resultado.get("score_confianca"),
                "recomendacao_cst": recomendacao.get("cst", ""),
                "recomendacao_cclasstrib": recomendacao.get("cclasstrib", ""),
                "requer_revisao_humana": "S" if resultado.get("necessita_validacao_humana", True) else "N",
                "resumo": resultado.get("resumo", ""),
                "resultado_json": resultado,
            }
        )
        analise = self.repository.obter_analise_ia(analise_id) or {}
        return {
            "contexto": contexto,
            "analise": analise,
        }
