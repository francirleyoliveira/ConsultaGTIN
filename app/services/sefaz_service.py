from __future__ import annotations

from datetime import datetime

import urllib3
from requests_pkcs12 import post

from app.config import Settings
from app.parsers.sefaz_xml import analisar_resposta_xml


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def montar_envelope_gtin(gtin: str) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">'
        "<soap12:Body>"
        '<ccgConsGTIN xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin">'
        "<nfeDadosMsg>"
        '<consGTIN versao="1.00" xmlns="http://www.portalfiscal.inf.br/nfe">'
        f"<GTIN>{gtin}</GTIN>"
        "</consGTIN>"
        "</nfeDadosMsg>"
        "</ccgConsGTIN>"
        "</soap12:Body>"
        "</soap12:Envelope>"
    )


def consultar_gtin_sefaz(gtin: str, settings: Settings) -> dict[str, str]:
    """Consulta um GTIN no webservice da Sefaz."""
    headers = {
        "Content-Type": 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"',
        "Host": "dfe-servico.svrs.rs.gov.br",
    }

    retorno = {
        "status": "Erro",
        "motivo": "Falha na conexao",
        "xProd": "",
        "NCM": "",
        "CEST": "",
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

    try:
        resposta = post(
            settings.url_webservice,
            data=montar_envelope_gtin(gtin).encode("utf-8"),
            headers=headers,
            pkcs12_filename=settings.cert_caminho,
            pkcs12_password=settings.cert_senha,
            verify=False,
            timeout=20,
        )

        if resposta.status_code == 200:
            return analisar_resposta_xml(resposta.text)

        retorno["motivo"] = f"HTTP {resposta.status_code}"
        return retorno
    except Exception as erro:
        retorno["motivo"] = str(erro)[:100]
        return retorno
