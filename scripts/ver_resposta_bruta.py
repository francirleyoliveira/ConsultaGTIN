from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from requests_pkcs12 import post

from app.config import DIAGNOSTICOS_DIR, load_settings
from app.services.sefaz_service import montar_envelope_gtin

GTIN_TESTE = "7891032015604"


def baixar_resposta_bruta(settings=None, gtin_teste: str = GTIN_TESTE, post_func=post):
    settings = settings or load_settings()
    headers = {
        'Content-Type': 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"',
        'Host': 'dfe-servico.svrs.rs.gov.br'
    }
    return post_func(
        settings.url_webservice,
        data=montar_envelope_gtin(gtin_teste).encode('utf-8'),
        headers=headers,
        pkcs12_filename=settings.cert_caminho,
        pkcs12_password=settings.cert_senha,
        verify=settings.requests_verify,
        timeout=30,
    )


def main() -> int:
    settings = load_settings()
    print(f"Consultando GTIN {GTIN_TESTE}...")

    try:
        resposta = baixar_resposta_bruta(settings=settings)

        DIAGNOSTICOS_DIR.mkdir(parents=True, exist_ok=True)
        nome_arquivo = DIAGNOSTICOS_DIR / "resposta_sefaz_bruta.xml"
        with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
            arquivo.write(resposta.text)

        print(f"\nSucesso! O arquivo '{nome_arquivo}' foi criado.")
        print("Abra-o para ver a estrutura completa da Sefaz.")
        return 0
    except Exception as erro:
        print(f"Erro: {erro}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
