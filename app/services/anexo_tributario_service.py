from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from requests_pkcs12 import get

from app.config import Settings


class AnexoTributarioIndisponivelError(RuntimeError):
    """Indica que a consulta de anexos do modulo C nao esta disponivel."""


class AnexoTributarioService:
    def __init__(
        self,
        settings: Settings,
        fetcher: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.fetcher = fetcher

    def sincronizar_anexos(self) -> list[dict[str, Any]]:
        if self.fetcher is not None:
            return self._normalizar_anexos(self.fetcher())
        if self.settings.cff_anexos_resposta_exemplo_path:
            caminho = Path(self.settings.cff_anexos_resposta_exemplo_path)
            if caminho.exists():
                return self._normalizar_anexos(json.loads(caminho.read_text(encoding="utf-8")))
        return self._consultar_api_anexos()

    def consultar_anexo(self, codigo_anexo: str) -> dict[str, Any]:
        codigo = str(codigo_anexo or "").strip()
        if not codigo:
            return {}
        for anexo in self.sincronizar_anexos():
            if anexo.get("anexo") == codigo:
                return anexo
        return {}

    def _consultar_api_anexos(self) -> list[dict[str, Any]]:
        try:
            resposta = get(
                self.settings.cff_anexos_api_url,
                pkcs12_filename=self.settings.cert_caminho,
                pkcs12_password=self.settings.cert_senha,
                timeout=30,
                verify=self.settings.requests_verify,
            )
            resposta.raise_for_status()
            return self._normalizar_anexos(resposta.json())
        except Exception as exc:
            raise AnexoTributarioIndisponivelError(
                f"Falha ao consultar os anexos tributarios com certificado: {exc}"
            ) from exc

    def _normalizar_anexos(self, raw: Any) -> list[dict[str, Any]]:
        if isinstance(raw, dict):
            raw = raw.get("anexos") or raw.get("Anexos") or raw.get("data") or []
        if not isinstance(raw, list):
            raise AnexoTributarioIndisponivelError("Resposta dos anexos em formato inesperado.")

        anexos_por_codigo: dict[str, dict[str, Any]] = {}
        anexos_sem_codigo: list[dict[str, Any]] = []
        for item in raw:
            dados = self._as_dict(item)
            codigo_anexo = self._extrair_codigo_anexo(dados)
            if self._tem_formato_flat(dados):
                anexo = anexos_por_codigo.setdefault(
                    codigo_anexo,
                    {
                        "anexo": codigo_anexo,
                        "descricao": str(dados.get("descrAnexo") or dados.get("DescricaoAnexo") or dados.get("descricao") or dados.get("Descricao") or "").strip(),
                        "publicacao": str(dados.get("Publicacao") or dados.get("publicacao") or "").strip(),
                        "inicio_vigencia": str(dados.get("dthIniVig") or dados.get("InicioVigencia") or dados.get("inicio_vigencia") or "").strip(),
                        "fim_vigencia": str(dados.get("dthFimVig") or dados.get("FimVigencia") or dados.get("fim_vigencia") or "").strip(),
                        "especificidades": [],
                        "raw_json": json.dumps(dados, ensure_ascii=False),
                    },
                )
                especificidade = self._normalizar_especificidade(dados)
                if any(especificidade.values()):
                    anexo["especificidades"].append(especificidade)
                continue

            especificidades_raw = (
                dados.get("Especificidades")
                or dados.get("especificidades")
                or dados.get("Detalhes")
                or dados.get("detalhes")
                or []
            )
            especificidades = [self._normalizar_especificidade(especificidade) for especificidade in especificidades_raw]
            anexo_normalizado = {
                "anexo": codigo_anexo,
                "descricao": str(
                    dados.get("DescricaoAnexo")
                    or dados.get("descricao")
                    or dados.get("Descricao")
                    or dados.get("descrAnexo")
                    or ""
                ).strip(),
                "publicacao": str(dados.get("Publicacao") or dados.get("publicacao") or "").strip(),
                "inicio_vigencia": str(dados.get("InicioVigencia") or dados.get("inicio_vigencia") or dados.get("dthIniVig") or "").strip(),
                "fim_vigencia": str(dados.get("FimVigencia") or dados.get("fim_vigencia") or dados.get("dthFimVig") or "").strip(),
                "especificidades": especificidades,
                "raw_json": json.dumps(dados, ensure_ascii=False),
            }
            if codigo_anexo:
                anexos_por_codigo[codigo_anexo] = anexo_normalizado
            else:
                anexos_sem_codigo.append(anexo_normalizado)
        return list(anexos_por_codigo.values()) + anexos_sem_codigo

    def _normalizar_especificidade(self, raw: Any) -> dict[str, Any]:
        dados = self._as_dict(raw)
        observacoes = [
            str(dados.get("texObservacao") or "").strip(),
            str(dados.get("descrCondicao") or "").strip(),
            str(dados.get("descrExcecao") or "").strip(),
        ]
        valor = " | ".join(item for item in observacoes if item)
        return {
            "codigo": str(
                dados.get("CodigoEspecificidade")
                or dados.get("codigo")
                or dados.get("Codigo")
                or dados.get("codNcmNbs")
                or ""
            ).strip(),
            "descricao": str(
                dados.get("DescricaoEspecificidade")
                or dados.get("descricao")
                or dados.get("Descricao")
                or dados.get("descrItemAnexo")
                or ""
            ).strip(),
            "valor": valor or str(dados.get("Valor") or dados.get("valor") or "").strip(),
            "tipo": str(dados.get("Tipo") or dados.get("tipo") or dados.get("TipoNomenclatura") or "").strip(),
            "publicacao": str(dados.get("Publicacao") or dados.get("publicacao") or "").strip(),
            "inicio_vigencia": str(dados.get("InicioVigencia") or dados.get("inicio_vigencia") or dados.get("dthIniVig") or "").strip(),
            "fim_vigencia": str(dados.get("FimVigencia") or dados.get("fim_vigencia") or dados.get("dthFimVig") or "").strip(),
            "raw_json": json.dumps(dados, ensure_ascii=False),
        }

    def _extrair_codigo_anexo(self, dados: dict[str, Any]) -> str:
        return str(
            dados.get("Anexo")
            or dados.get("anexo")
            or dados.get("Codigo")
            or dados.get("codigo")
            or dados.get("nroAnexo")
            or ""
        ).strip()

    def _tem_formato_flat(self, dados: dict[str, Any]) -> bool:
        return any(chave in dados for chave in ("nroAnexo", "codNcmNbs", "descrItemAnexo", "descrAnexo"))

    def _as_dict(self, raw: Any) -> dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        if hasattr(raw, "__dict__"):
            return {key: value for key, value in vars(raw).items() if not key.startswith("_")}
        return {}
