from __future__ import annotations

from datetime import datetime
import xml.etree.ElementTree as ET



def analisar_resposta_xml(xml_texto: str) -> dict[str, str]:
    """Extrai os principais campos da resposta XML da Sefaz."""
    retorno = {
        "status": "Erro XML",
        "motivo": "Resposta invalida",
        "xProd": "",
        "NCM": "",
        "CEST": "",
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

    try:
        root = ET.fromstring(xml_texto)

        mapa_tags: dict[str, str | None] = {}
        for elem in root.iter():
            tag_pura = elem.tag.split("}")[-1]
            mapa_tags[tag_pura] = elem.text

        if "cStat" in mapa_tags:
            retorno["status"] = mapa_tags["cStat"] or ""
            retorno["motivo"] = mapa_tags.get("xMotivo") or ""

            if retorno["status"] in ["949", "9490"]:
                retorno["xProd"] = mapa_tags.get("xProd") or ""
                retorno["NCM"] = mapa_tags.get("NCM") or ""
                retorno["CEST"] = mapa_tags.get("CEST") or ""

                dh_resp = mapa_tags.get("dhResp")
                if dh_resp:
                    try:
                        dt_obj = datetime.fromisoformat(dh_resp.replace("Z", "+00:00"))
                        retorno["data_hora"] = dt_obj.strftime("%d/%m/%Y %H:%M:%S")
                    except ValueError:
                        retorno["data_hora"] = dh_resp

        return retorno
    except ET.ParseError as erro:
        retorno["motivo"] = f"Erro Parse: {erro}"
        return retorno
