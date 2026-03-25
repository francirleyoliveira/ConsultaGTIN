from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from requests_pkcs12 import get

from app.config import Settings


class ModuloCIndisponivelError(RuntimeError):
    """Indica que o catalogo tributario do modulo C nao esta disponivel."""


class DossieTributarioService:
    def __init__(
        self,
        settings: Settings,
        fetcher: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.fetcher = fetcher

    def sincronizar_catalogo(self) -> list[dict[str, Any]]:
        if self.fetcher is not None:
            return self._normalizar_catalogo(self.fetcher())
        if self.settings.cff_resposta_exemplo_path:
            caminho = Path(self.settings.cff_resposta_exemplo_path)
            if caminho.exists():
                return self._normalizar_catalogo(json.loads(caminho.read_text(encoding="utf-8")))
        return self._consultar_api_catalogo()

    def consultar_cclasstrib(self, cclasstrib: str) -> dict[str, Any]:
        codigo = str(cclasstrib or "").strip()
        if not codigo:
            return {}
        catalogo = self.sincronizar_catalogo()
        for cst in catalogo:
            for classificacao in cst.get("classificacoes_tributarias", []):
                if classificacao.get("cclasstrib") == codigo:
                    return classificacao
        return {}

    def _consultar_api_catalogo(self) -> list[dict[str, Any]]:
        try:
            resposta = get(
                self.settings.cff_api_url,
                pkcs12_filename=self.settings.cert_caminho,
                pkcs12_password=self.settings.cert_senha,
                timeout=30,
                verify=False,
            )
            resposta.raise_for_status()
            return self._normalizar_catalogo(resposta.json())
        except Exception as exc:
            raise ModuloCIndisponivelError(f"Falha ao consultar o catalogo tributario com certificado: {exc}") from exc

    def _normalizar_catalogo(self, raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            raise ModuloCIndisponivelError("Resposta do modulo C em formato inesperado.")

        catalogo: list[dict[str, Any]] = []
        for item_cst in raw:
            cst = self._as_dict(item_cst)
            classificacoes = cst.get("classificacoesTributarias") or cst.get("classificacoes_tributarias") or []
            classificacoes_normalizadas = [self._normalizar_classificacao(item, cst) for item in classificacoes]
            catalogo.append(
                {
                    "cst": str(cst.get("CST") or cst.get("cst") or "").strip(),
                    "descricao_cst": str(cst.get("DescricaoCST") or cst.get("descricao_cst") or "").strip(),
                    "ind_ibscbs": self._bool_text(cst.get("IndIBSCBS")),
                    "ind_red_bc": self._bool_text(cst.get("IndRedBC")),
                    "ind_red_aliq": self._bool_text(cst.get("IndRedAliq")),
                    "ind_transf_cred": self._bool_text(cst.get("IndTransfCred")),
                    "ind_dif": self._bool_text(cst.get("IndDif")),
                    "ind_ajuste_compet": self._bool_text(cst.get("IndAjusteCompet")),
                    "ind_ibscbs_mono": self._bool_text(cst.get("IndIBSCBSMono")),
                    "ind_cred_pres_ibs_zfm": self._bool_text(cst.get("IndCredPresIBSZFM")),
                    "publicacao": str(cst.get("Publicacao") or "").strip(),
                    "inicio_vigencia": str(cst.get("InicioVigencia") or "").strip(),
                    "fim_vigencia": str(cst.get("FimVigencia") or "").strip(),
                    "classificacoes_tributarias": classificacoes_normalizadas,
                    "raw_json": json.dumps(cst, ensure_ascii=False),
                }
            )
        return catalogo

    def _normalizar_classificacao(self, raw: Any, cst: dict[str, Any]) -> dict[str, Any]:
        dados = self._as_dict(raw)
        link = str(dados.get("Link") or dados.get("link") or "").strip()
        return {
            "cst": str(cst.get("CST") or cst.get("cst") or "").strip(),
            "cclasstrib": str(dados.get("cClassTrib") or dados.get("cclasstrib") or "").strip(),
            "descricao": str(dados.get("DescricaoClassTrib") or dados.get("descricao") or "").strip(),
            "p_red_ibs": str(dados.get("pRedIBS") or "").strip(),
            "p_red_cbs": str(dados.get("pRedCBS") or "").strip(),
            "ind_trib_regular": self._bool_text(dados.get("IndTribRegular")),
            "ind_cred_pres_oper": self._bool_text(dados.get("IndCredPresOper")),
            "ind_estorno_cred": self._bool_text(dados.get("IndEstornoCred")),
            "monofasia_sujeita_retencao": self._bool_text(dados.get("MonofasiaSujeitaRetencao")),
            "monofasia_retida_ant": self._bool_text(dados.get("MonofasiaRetidaAnt")),
            "monofasia_diferimento": self._bool_text(dados.get("MonofasiaDiferimento")),
            "monofasia_padrao": self._bool_text(dados.get("MonofasiaPadrao")),
            "publicacao": str(dados.get("Publicacao") or "").strip(),
            "inicio_vigencia": str(dados.get("InicioVigencia") or "").strip(),
            "fim_vigencia": str(dados.get("FimVigencia") or "").strip(),
            "tipo_aliquota": str(dados.get("TipoAliquota") or "").strip(),
            "ind_nfe": self._bool_text(dados.get("IndNFe")),
            "ind_nfce": self._bool_text(dados.get("IndNFCe")),
            "ind_cte": self._bool_text(dados.get("IndCTe")),
            "ind_cteos": self._bool_text(dados.get("IndCTeOS")),
            "ind_bpe": self._bool_text(dados.get("IndBPe")),
            "ind_nf3e": self._bool_text(dados.get("IndNF3e")),
            "ind_nfcom": self._bool_text(dados.get("IndNFCom")),
            "ind_nfse": self._bool_text(dados.get("IndNFSE")),
            "ind_bpetm": self._bool_text(dados.get("IndBPeTM")),
            "ind_bpeta": self._bool_text(dados.get("IndBPeTA")),
            "ind_nfag": self._bool_text(dados.get("IndNFAg")),
            "ind_nfsvia": self._bool_text(dados.get("IndNFSVIA")),
            "ind_nfabi": self._bool_text(dados.get("IndNFABI")),
            "ind_nfgas": self._bool_text(dados.get("IndNFGas")),
            "ind_dere": self._bool_text(dados.get("IndDERE")),
            "anexo": str(dados.get("Anexo") or "").strip(),
            "link": link,
            "base_legal": link,
            "links_legais": [link] if link else [],
            "raw_json": json.dumps(dados, ensure_ascii=False),
        }

    def _bool_text(self, valor: Any) -> str:
        if isinstance(valor, bool):
            return "1" if valor else "0"
        if valor in (None, ""):
            return ""
        texto = str(valor).strip().lower()
        if texto in {"true", "1", "sim", "s"}:
            return "1"
        if texto in {"false", "0", "nao", "não", "n"}:
            return "0"
        return str(valor).strip()

    def _as_dict(self, raw: Any) -> dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        if hasattr(raw, "__dict__"):
            return {key: value for key, value in vars(raw).items() if not key.startswith("_")}
        return {}
