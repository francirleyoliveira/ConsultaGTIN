from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable

from app.config import Settings
from app.services.anexo_tributario_service import AnexoTributarioIndisponivelError, AnexoTributarioService
from app.services.conformidade_scraper_service import ConformidadeScraperService, ModuloBIndisponivelError
from app.services.dossie_tributario_service import DossieTributarioService, ModuloCIndisponivelError
from app.services.sefaz_service import consultar_gtin_sefaz
from app.services.sqlite_service import ConsultaGtinRepository
from app.validators.gtin import comparar_ncm, validar_digito_gtin, validar_prefixo_gs1_brasil


STATUS_SUCESSO_SEFAZ = {"949", "9490"}


class ClassificacaoTributariaService:
    def __init__(
        self,
        settings: Settings,
        repository: ConsultaGtinRepository,
        sefaz_consultor: Callable[[str, Settings], dict[str, str]] = consultar_gtin_sefaz,
        scraper_service: ConformidadeScraperService | None = None,
        dossie_service: DossieTributarioService | None = None,
        anexo_service: AnexoTributarioService | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.sefaz_consultor = sefaz_consultor
        self.scraper_service = scraper_service or ConformidadeScraperService(settings)
        self.dossie_service = dossie_service or DossieTributarioService(settings)
        self.anexo_service = anexo_service or AnexoTributarioService(settings)

    def processar_produto(self, produto: tuple[Any, Any, Any]) -> dict[str, Any]:
        codprod, gtin, ncm_erp = produto[0], str(produto[1] or ""), str(produto[2] or "")
        consulta = self._consultar_gtin(codprod, gtin, ncm_erp)
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
        resultado = {"ncm": ncm_para_consulta, "cenarios": [], "warnings": []}
        if not ncm_para_consulta:
            self.repository.salvar_cenarios_tributarios("", [])
            return resultado

        try:
            cenarios_modulo_b = self.scraper_service.buscar_cenarios_por_ncm(ncm_para_consulta)
        except ModuloBIndisponivelError as exc:
            resultado["warnings"].append(str(exc))
            self.repository.salvar_cenarios_tributarios(ncm_para_consulta, [])
            return resultado

        catalogo_sincronizado = False
        anexos_sincronizados = False
        cenarios_enriquecidos: list[dict[str, Any]] = []
        for cenario in cenarios_modulo_b:
            cclasstrib = str(cenario.get("cclasstrib", "") or "").strip()
            dossie = self.repository.obter_dossie_cache(cclasstrib) if cclasstrib else None
            if cclasstrib and not dossie and not catalogo_sincronizado:
                try:
                    catalogo = self.dossie_service.sincronizar_catalogo()
                    self.repository.salvar_catalogo_tributario(catalogo)
                    catalogo_sincronizado = True
                    dossie = self.repository.obter_dossie_cache(cclasstrib)
                except ModuloCIndisponivelError as exc:
                    resultado["warnings"].append(str(exc))
                    dossie = {}
            elif cclasstrib and not dossie:
                dossie = self.repository.obter_dossie_cache(cclasstrib) or {}

            codigo_anexo = str((dossie or {}).get("anexo", "") or "").strip()
            if codigo_anexo and not self.repository.obter_anexo_cache(codigo_anexo) and not anexos_sincronizados:
                try:
                    anexos = self.anexo_service.sincronizar_anexos()
                    self.repository.salvar_anexos_tributarios(anexos, substituir=True)
                    anexos_sincronizados = True
                except AnexoTributarioIndisponivelError as exc:
                    resultado["warnings"].append(str(exc))

            cenarios_enriquecidos.append(
                {
                    "ncm": cenario.get("ncm", ncm_para_consulta),
                    "cst": cenario.get("cst", ""),
                    "cclasstrib": cclasstrib,
                    "condicao_legal": cenario.get("condicao_legal", ""),
                    "descricao_dossie": (dossie or {}).get("descricao", ""),
                    "p_red_ibs": (dossie or {}).get("p_red_ibs", ""),
                    "p_red_cbs": (dossie or {}).get("p_red_cbs", ""),
                    "publicacao": (dossie or {}).get("publicacao", ""),
                    "inicio_vigencia": (dossie or {}).get("inicio_vigencia", ""),
                    "anexo": (dossie or {}).get("anexo", ""),
                    "ind_nfe": (dossie or {}).get("ind_nfe", ""),
                    "ind_nfce": (dossie or {}).get("ind_nfce", ""),
                    "base_legal": (dossie or {}).get("base_legal", ""),
                    "fonte": cenario.get("fonte", "portal_conformidade_facil"),
                }
            )

        self.repository.salvar_cenarios_tributarios(ncm_para_consulta, cenarios_enriquecidos)
        resultado["cenarios"] = cenarios_enriquecidos
        return resultado

    def _consultar_gtin(self, codprod: Any, gtin: str, ncm_erp: str) -> dict[str, str]:
        ncm_erp_limpo = "".join(filter(str.isdigit, str(ncm_erp or "")))
        if not validar_digito_gtin(gtin):
            return self._montar_consulta(codprod, gtin, ncm_erp_limpo, "GTIN_INVALIDO", f"Digito Verificador (DV) do GTIN {gtin} esta incorreto.")
        if not validar_prefixo_gs1_brasil(gtin):
            return self._montar_consulta(codprod, gtin, ncm_erp_limpo, "GTIN_FORA_GS1_BR", "GTIN fora do prefixo GS1 Brasil (789/790).")
        resposta = self._consultar_sefaz_com_retry(gtin)
        ncm_sefaz = str(resposta.get("NCM", "") or "").strip()
        status = str(resposta.get("status", "") or "")
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
            "motivo_sefaz": resposta.get("motivo", ""),
            "ncm_winthor": ncm_erp_limpo,
            "ncm_oficial": ncm_sefaz,
            "divergencia_ncm": divergencia,
            "descricao_produto": resposta.get("xProd", ""),
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

    def _montar_consulta(self, codprod: Any, gtin: str, ncm_erp: str, status: str, motivo: str) -> dict[str, str]:
        divergencia = "GTIN COM DIGITO INVALIDO" if status == "GTIN_INVALIDO" else "GTIN FORA DO ESCOPO GS1 BR"
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
            "cest": "",
        }
