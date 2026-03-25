from app.config import DIAGNOSTICOS_DIR, load_settings
from app.parsers.sefaz_xml import analisar_resposta_xml
from app.services.oracle_service import buscar_gtins_winthor as _buscar_gtins_winthor
from app.services.sefaz_service import consultar_gtin_sefaz as _consultar_gtin_sefaz
from app.validators.gtin import validar_digito_gtin


_settings = load_settings()


def buscar_gtins_winthor():
    return _buscar_gtins_winthor(_settings)


def consultar_gtin_sefaz(gtin):
    return _consultar_gtin_sefaz(gtin, _settings)


if __name__ == "__main__":
    caminho_teste = DIAGNOSTICOS_DIR / "resposta_sefaz_bruta.xml"
    if caminho_teste.exists():
        with open(caminho_teste, "r", encoding="utf-8") as arquivo:
            print(analisar_resposta_xml(arquivo.read()))
    else:
        print(consultar_gtin_sefaz("7891000315507"))
