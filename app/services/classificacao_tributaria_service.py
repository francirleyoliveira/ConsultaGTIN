from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable

from app.config import Settings
from app.services.anexo_tributario_service import AnexoTributarioIndisponivelError, AnexoTributarioService
from app.services.conformidade_scraper_service import ConformidadeScraperService, ModuloBIndisponivelError
from app.services.dossie_tributario_service import DossieTributarioService, ModuloCIndisponivelError
from app.services.gtin_health_service import GtinServiceHealthMonitor
from app.services.sefaz_service import consultar_gtin_sefaz
from app.services.sqlite_service import ConsultaGtinRepository
from app.validators.gtin import comparar_ncm, validar_digito_gtin, validar_prefixo_gs1_brasil


STATUS_SUCESSO_SEFAZ = {"949", "9490"}
STATUS_FALHA_COMUNICACAO = {"Erro", "SEFAZ_INDISPONIVEL"}


class ClassificacaoTributariaService:
    def __init__(
        self,
        settings: Settings,
        repository: ConsultaGtinRepository,
        sefaz_consultor: Callable[[str, Settings], dict[str, str]] = consultar_gtin_sefaz,
        scraper_service: ConformidadeScraperService | None = None,
        dossie_service: DossieTributarioService | None = None,
        anexo_service: AnexoTributarioService | None = None,
        gtin_health_monitor: GtinServiceHealthMonitor | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.sefaz_consultor = sefaz_consultor
        self.scraper_service = scraper_service or ConformidadeScraperService(settings)
        self.dossie_service = dossie_service or DossieTributarioService(settings)
        self.anexo_service = anexo_service or AnexoTributarioService(settings)
        self.gtin_health_monitor = gtin_health_monitor or GtinServiceHealthMonitor(settings)

    def verificar_saude_servico_gtin(self, force: bool = False) -> dict[str, Any]:
        return self.gtin_health_monitor.verificar(force=force)

    def circuito_gtin_aberto(self) -> bool:
        return self.gtin_health_monitor.esta_bloqueado()

    def processar_produto(self, produto: tuple[Any, ...]) -> dict[str, Any]:
        codprod, gtin, ncm_erp = produto[0], str(produto[1] or ""), str(produto[2] or "")
        descricao_erp = str(produto[3] or "") if len(produto) > 3 else ""
        consulta = self._consultar_gtin(codprod, gtin, ncm_erp, descricao_erp)
        self.repository.upsert_consulta(consulta)

        ncm_para_consulta = consulta["ncm_winthor"] or consulta["ncm_oficial"]
        resultado_ncm = self.processar_ncm(ncm_para_consulta)
        return {
            "consulta": consulta,
            "cenarios": resultado_ncm["cenarios"],
            "warnings": resultado_ncm["warnings"],
        }

    def processar_ncm(self, ncm: str) -> dict[str, Any]:
        ncm_para_consulta = "".join(filter(str.isdigit, str(ncm or "")))
        resultado = {"ncm": ncm_para_consulta, "cenarios": [], "warnings": [], "origem_cenarios": "atualizado"}
        if not ncm_para_consulta:
            resultado["origem_cenarios"] = "vazio"
            return resultado

        try:
            cenarios_modulo_b = self.scraper_service.buscar_cenarios_por_ncm(ncm_para_consulta)
        except ModuloBIndisponivelError as exc:
            mensagem = str(exc)
            resultado["warnings"].append(mensagem)
            if "Nenhum cenario" in mensagem:
                resultado["origem_cenarios"] = "limpo"
                self.repository.salvar_cenarios_tributarios(ncm_para_consulta, [])
            else:
                resultado["origem_cenarios"] = "cache"
                cenarios_cache = self.repository.listar_cenarios_tributarios({"ncm": ncm_para_consulta})
                if cenarios_cache:
                    resultado["cenarios"] = cenarios_cache
                    resultado["warnings"].append("Cenarios existentes no cache local foram preservados.")
            return resultado

        cenarios_base = [
            {
                "ncm": cenario.get("ncm", ncm_para_consulta),
                "cst": cenario.get("cst", ""),
                "cclasstrib": str(cenario.get("cclasstrib", "") or "").strip(),
                "condicao_legal": cenario.get("condicao_legal", ""),
                "fonte": cenario.get("fonte", "portal_conformidade_facil"),
            }
            for cenario in cenarios_modulo_b
        ]

        cclasstribs_pendentes = {
            item["cclasstrib"]
            for item in cenarios_base
            if item["cclasstrib"] and not self.repository.obter_dossie_cache(item["cclasstrib"])
        }
        if cclasstribs_pendentes:
            try:
                catalogo = self.dossie_service.sincronizar_catalogo()
                self.repository.salvar_catalogo_tributario(catalogo)
            except ModuloCIndisponivelError as exc:
                resultado["warnings"].append(str(exc))

        anexos_pendentes = set()
        for item in cenarios_base:
            cclasstrib = item["cclasstrib"]
            if not cclasstrib:
                continue
            dossie = self.repository.obter_dossie_cache(cclasstrib) or {}
            codigo_anexo = str(dossie.get("anexo", "") or "").strip()
            if codigo_anexo and not self.repository.obter_anexo_cache(codigo_anexo):
                anexos_pendentes.add(codigo_anexo)

        if anexos_pendentes:
            try:
                anexos = self.anexo_service.sincronizar_anexos()
                if anexos:
                    self.repository.salvar_anexos_tributarios(anexos, substituir=True)
                else:
                    resultado["warnings"].append("Servico de anexos retornou vazio; cache local preservado.")
            except AnexoTributarioIndisponivelError as exc:
                resultado["warnings"].append(str(exc))

        self.repository.salvar_cenarios_tributarios(ncm_para_consulta, cenarios_base)
        resultado["cenarios"] = self.repository.listar_cenarios_tributarios({"ncm": ncm_para_consulta})
        return resultado

    def _consultar_gtin(self, codprod: Any, gtin: str, ncm_erp: str, descricao_erp: str = "") -> dict[str, str]:
        ncm_erp_limpo = "".join(filter(str.isdigit, str(ncm_erp or "")))
        if not validar_digito_gtin(gtin):
            return self._montar_consulta(codprod, gtin, ncm_erp_limpo, "GTIN_INVALIDO", f"Digito Verificador (DV) do GTIN {gtin} esta incorreto.", descricao_erp)
        if not validar_prefixo_gs1_brasil(gtin):
            return self._montar_consulta(codprod, gtin, ncm_erp_limpo, "GTIN_FORA_GS1_BR", "GTIN fora do prefixo GS1 Brasil (789/790).", descricao_erp)

        consulta_cache = self.repository.obter_consulta_por_gtin(gtin)
        disponibilidade = self.gtin_health_monitor.verificar(force=False)
        if not disponibilidade.get("ok"):
            return self._montar_consulta_com_cache_oficial(
                codprod,
                gtin,
                ncm_erp_limpo,
                "SEFAZ_INDISPONIVEL",
                str(disponibilidade.get("motivo") or disponibilidade.get("message") or "Servico GTIN indisponivel."),
                consulta_cache,
                descricao_erp,
            )

        resposta = self._consultar_sefaz_com_retry(gtin)
        self.gtin_health_monitor.registrar_resultado_consulta(resposta)
        ncm_sefaz = str(resposta.get("NCM", "") or "").strip()
        status = str(resposta.get("status", "") or "")
        motivo = str(resposta.get("motivo", "") or "")

        if status in STATUS_FALHA_COMUNICACAO:
            return self._montar_consulta_com_cache_oficial(codprod, gtin, ncm_erp_limpo, status, motivo, consulta_cache, descricao_erp)

        if status in STATUS_SUCESSO_SEFAZ:
            divergencia = comparar_ncm(ncm_erp_limpo, ncm_sefaz)
        elif status == "GTIN_INVALIDO":
            divergencia = "GTIN COM DIGITO INVALIDO"
        elif status == "GTIN_FORA_GS1_BR":
            divergencia = "GTIN FORA DO ESCOPO GS1 BR"
        else:
            divergencia = "CONSULTA FALHOU"
        return {
            "cod_winthor": str(codprod),
            "gtin": gtin,
            "data_hora_resposta": resposta.get("data_hora", datetime.now().strftime("%d/%m/%Y %H:%M:%S")),
            "status_sefaz": status,
            "motivo_sefaz": motivo,
            "ncm_winthor": ncm_erp_limpo,
            "ncm_oficial": ncm_sefaz,
            "divergencia_ncm": divergencia,
            "descricao_produto": resposta.get("xProd", ""),
            "descricao_erp": descricao_erp,
            "cest": resposta.get("CEST", ""),
        }

    def _consultar_sefaz_com_retry(self, gtin: str, tentativas: int = 2, pausa_segundos: float = 1.5) -> dict[str, str]:
        ultima_resposta: dict[str, str] = {}
        for tentativa in range(1, tentativas + 1):
            ultima_resposta = self.sefaz_consultor(gtin, self.settings)
            if str(ultima_resposta.get("status", "")) != "656":
                return ultima_resposta
            if tentativa < tentativas:
                time.sleep(pausa_segundos)
        return ultima_resposta

    def _montar_consulta_com_cache_oficial(
        self,
        codprod: Any,
        gtin: str,
        ncm_erp: str,
        status: str,
        motivo: str,
        consulta_cache: dict[str, Any] | None,
        descricao_erp: str = "",
    ) -> dict[str, str]:
        if not consulta_cache:
            return self._montar_consulta(codprod, gtin, ncm_erp, status, motivo, descricao_erp)

        ncm_oficial_cache = str(consulta_cache.get("ncm_oficial", "") or "").strip()
        if not ncm_oficial_cache:
            return self._montar_consulta(codprod, gtin, ncm_erp, status, motivo, descricao_erp)

        data_cache = str(consulta_cache.get("data_hora_resposta", "") or "").strip()
        motivo_cache = motivo.strip()
        complemento = f"Usando cache oficial GS1 da consulta em {data_cache}." if data_cache else "Usando cache oficial GS1 da ultima consulta valida."
        motivo_final = f"{motivo_cache} {complemento}".strip() if motivo_cache else complemento
        return {
            "cod_winthor": str(codprod),
            "gtin": gtin,
            "data_hora_resposta": data_cache or datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "status_sefaz": status,
            "motivo_sefaz": motivo_final,
            "ncm_winthor": ncm_erp,
            "ncm_oficial": ncm_oficial_cache,
            "divergencia_ncm": comparar_ncm(ncm_erp, ncm_oficial_cache),
            "descricao_produto": str(consulta_cache.get("descricao_produto", "") or ""),
            "descricao_erp": descricao_erp or str(consulta_cache.get("descricao_erp", "") or ""),
            "cest": str(consulta_cache.get("cest", "") or ""),
        }

    def _montar_consulta(self, codprod: Any, gtin: str, ncm_erp: str, status: str, motivo: str, descricao_erp: str = "") -> dict[str, str]:
        if status == "GTIN_INVALIDO":
            divergencia = "GTIN COM DIGITO INVALIDO"
        elif status == "GTIN_FORA_GS1_BR":
            divergencia = "GTIN FORA DO ESCOPO GS1 BR"
        else:
            divergencia = "CONSULTA FALHOU"
        return {
            "cod_winthor": str(codprod),
            "gtin": gtin,
            "data_hora_resposta": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "status_sefaz": status,
            "motivo_sefaz": motivo,
            "ncm_winthor": ncm_erp,
            "ncm_oficial": "",
            "divergencia_ncm": divergencia,
            "descricao_produto": "",
            "descricao_erp": descricao_erp,
            "cest": "",
        }
