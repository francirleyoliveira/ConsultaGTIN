from requests_pkcs12 import post

from app.config import DIAGNOSTICOS_DIR, load_settings
from app.services.sefaz_service import montar_envelope_gtin

GTIN_TESTE = "7891032015604"
settings = load_settings()

headers = {
    'Content-Type': 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"',
    'Host': 'dfe-servico.svrs.rs.gov.br'
}

print(f"Consultando GTIN {GTIN_TESTE}...")

try:
    resposta = post(
        settings.url_webservice,
        data=montar_envelope_gtin(GTIN_TESTE).encode('utf-8'),
        headers=headers,
        pkcs12_filename=settings.cert_caminho,
        pkcs12_password=settings.cert_senha,
        verify=False,
        timeout=30
    )

    DIAGNOSTICOS_DIR.mkdir(parents=True, exist_ok=True)
    nome_arquivo = DIAGNOSTICOS_DIR / "resposta_sefaz_bruta.xml"
    with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
        arquivo.write(resposta.text)

    print(f"\nSucesso! O arquivo '{nome_arquivo}' foi criado.")
    print("Abra-o para ver a estrutura completa da Sefaz.")

except Exception as erro:
    print(f"Erro: {erro}")
