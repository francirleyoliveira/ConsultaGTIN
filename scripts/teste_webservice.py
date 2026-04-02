import xml.etree.ElementTree as ET
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from requests_pkcs12 import post

from app.config import DIAGNOSTICOS_DIR, load_settings
from app.services.sefaz_service import montar_envelope_gtin


GTIN_TESTE = "7891032015604"


def executar_teste_webservice(settings=None, gtin_teste: str = GTIN_TESTE, post_func=post):
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
    print("=" * 60)
    print("  TESTE DE CONEXAO - WebService Sefaz (CCG ConsGTIN)")
    print("=" * 60)

    settings = load_settings()
    print(f"   Certificado: {settings.cert_caminho}")
    print("\n[3/4] Enviando requisicao minificada...")

    try:
        resposta = executar_teste_webservice(settings=settings)
        print(f"   Status HTTP: {resposta.status_code}")
    except Exception as erro:
        print(f"   ERRO na conexao: {erro}")
        return 1

    print("\n[4/4] Analisando resposta da Sefaz...")
    if resposta.status_code == 200:
        print("   SUCESSO! Conexao estabelecida.")
        try:
            root = ET.fromstring(resposta.text)
            c_stat = root.find('.//{http://www.portalfiscal.inf.br/nfe}cStat')
            x_motivo = root.find('.//{http://www.portalfiscal.inf.br/nfe}xMotivo')

            if c_stat is not None:
                print(f"   GTIN: {GTIN_TESTE}")
                print(f"   Status: {c_stat.text}")
                print(f"   Motivo: {x_motivo.text if x_motivo is not None else 'N/A'}")

                if c_stat.text == "949":
                    x_prod = root.find('.//{http://www.portalfiscal.inf.br/nfe}xProd')
                    print(f"   Produto: {x_prod.text if x_prod is not None else 'N/A'}")
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
