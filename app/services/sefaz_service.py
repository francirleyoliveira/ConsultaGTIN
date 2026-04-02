from __future__ import annotations

from datetime import datetime
from typing import Any
from xml.sax.saxutils import escape

from requests_pkcs12 import post

from app.config import Settings
from app.parsers.sefaz_xml import analisar_resposta_xml


GTIN_XML_VERSAO = "1.00"



def montar_envelope_gtin(gtin: str, versao: str = GTIN_XML_VERSAO) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">'
        "<soap12:Body>"
        '<ccgConsGTIN xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin">'
        "<nfeDadosMsg>"
        f'<consGTIN versao="{versao}" xmlns="http://www.portalfiscal.inf.br/nfe">'
        f"<GTIN>{escape(gtin)}</GTIN>"
        "</consGTIN>"
        "</nfeDadosMsg>"
        "</ccgConsGTIN>"
        "</soap12:Body>"
        "</soap12:Envelope>"
    )



def montar_headers_gtin() -> dict[str, str]:
    return {
        "Content-Type": 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"',
        "Host": "dfe-servico.svrs.rs.gov.br",
    }



def consultar_gtin_sefaz(gtin: str, settings: Settings, timeout_seconds: int | float = 20) -> dict[str, str]:
    """Consulta um GTIN no webservice da Sefaz."""
    retorno = {
        "status": "Erro",
        "motivo": "Falha na conexao",
        "xProd": "",
        "NCM": "",
        "CEST": "",
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "gtin": gtin,
    }

    try:
        resposta = post(
            settings.url_webservice,
            data=montar_envelope_gtin(gtin).encode("utf-8"),
            headers=montar_headers_gtin(),
            pkcs12_filename=settings.cert_caminho,
            pkcs12_password=settings.cert_senha,
            verify=settings.requests_verify,
            timeout=timeout_seconds,
        )

        if resposta.status_code == 200:
            payload = analisar_resposta_xml(resposta.text)
            payload["gtin"] = gtin
            return payload

        retorno["motivo"] = f"HTTP {resposta.status_code}"
        return retorno
    except Exception as erro:
        retorno["motivo"] = str(erro)[:180]
        return retorno



def realizar_healthcheck_gtin(
    settings: Settings,
    gtin_teste: str | None = None,
    timeout_seconds: int | float | None = None,
) -> dict[str, Any]:
    gtin = "".join(filter(str.isdigit, str(gtin_teste or settings.gtin_healthcheck_gtin or "")))
    if not gtin:
        return {
            "ok": False,
            "status": "CONFIG",
            "motivo": "GTIN de healthcheck nao configurado.",
            "gtin_teste": "",
            "checked_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        }

    timeout = timeout_seconds if timeout_seconds is not None else settings.gtin_healthcheck_timeout_seconds
    resposta = consultar_gtin_sefaz(gtin, settings, timeout_seconds=timeout)
    status = str(resposta.get("status") or "").strip()
    motivo = str(resposta.get("motivo") or "").strip()
    return {
        "ok": status.startswith("949"),
        "status": status,
        "motivo": motivo,
        "gtin_teste": gtin,
        "checked_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }
