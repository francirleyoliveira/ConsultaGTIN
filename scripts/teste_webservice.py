import xml.etree.ElementTree as ET
from pathlib import Path

from requests_pkcs12 import post

from app.config import DIAGNOSTICOS_DIR, load_settings
from app.services.sefaz_service import montar_envelope_gtin


print("=" * 60)
print("  TESTE DE CONEXAO - WebService Sefaz (CCG ConsGTIN)")
print("=" * 60)

settings = load_settings()

print(f"   Certificado: {settings.cert_caminho}")
print("\n[3/4] Enviando requisicao minificada...")

GTIN_TESTE = "7891032015604"

headers = {
    'Content-Type': 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"',
    'Host': 'dfe-servico.svrs.rs.gov.br'
}

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
    print(f"   Status HTTP: {resposta.status_code}")
except Exception as erro:
    print(f"   ERRO na conexao: {erro}")
    raise SystemExit(1)

print("\n[4/4] Analisando resposta da Sefaz...")
if resposta.status_code == 200:
    print("   SUCESSO! Conexao estabelecida.")
    try:
        root = ET.fromstring(resposta.text)
        cStat = root.find('.//{http://www.portalfiscal.inf.br/nfe}cStat')
        xMotivo = root.find('.//{http://www.portalfiscal.inf.br/nfe}xMotivo')

        if cStat is not None:
            print(f"   GTIN: {GTIN_TESTE}")
            print(f"   Status: {cStat.text}")
            print(f"   Motivo: {xMotivo.text if xMotivo is not None else 'N/A'}")

            if cStat.text == "949":
                xProd = root.find('.//{http://www.portalfiscal.inf.br/nfe}xProd')
                print(f"   Produto: {xProd.text if xProd is not None else 'N/A'}")
        else:
            print("   Resposta sem cStat. Verifique o XML abaixo:")
            print(f"   {resposta.text[:500]}")
    except Exception as erro:
        print(f"   Erro ao processar XML: {erro}")
else:
    print(f"   Falha com Status {resposta.status_code}")
    print(f"   Resposta: {resposta.text[:500]}")

DIAGNOSTICOS_DIR.mkdir(parents=True, exist_ok=True)
(Path(DIAGNOSTICOS_DIR) / "resultado_teste.txt").write_text("Teste executado.\n", encoding="utf-8")

print("\n" + "=" * 60)
