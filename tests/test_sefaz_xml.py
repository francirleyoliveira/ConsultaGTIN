from __future__ import annotations

import unittest

from app.parsers.sefaz_xml import analisar_resposta_xml


class SefazXmlParserTest(unittest.TestCase):
    def test_parseia_data_com_offset_negativo_sem_split_fixo(self) -> None:
        xml = """<?xml version='1.0' encoding='utf-8'?>
        <retConsGTIN xmlns='http://www.portalfiscal.inf.br/nfe'>
            <cStat>9490</cStat>
            <xMotivo>Consulta realizada com sucesso</xMotivo>
            <xProd>Produto Teste</xProd>
            <NCM>22030000</NCM>
            <CEST>1234567</CEST>
            <dhResp>2026-03-26T10:15:00-02:00</dhResp>
        </retConsGTIN>
        """

        resultado = analisar_resposta_xml(xml)

        self.assertEqual('9490', resultado['status'])
        self.assertEqual('26/03/2026 10:15:00', resultado['data_hora'])

    def test_mantem_data_original_quando_iso_e_invalido(self) -> None:
        xml = """<?xml version='1.0' encoding='utf-8'?>
        <retConsGTIN xmlns='http://www.portalfiscal.inf.br/nfe'>
            <cStat>9490</cStat>
            <xMotivo>Consulta realizada com sucesso</xMotivo>
            <dhResp>valor-invalido</dhResp>
        </retConsGTIN>
        """

        resultado = analisar_resposta_xml(xml)

        self.assertEqual('valor-invalido', resultado['data_hora'])


if __name__ == '__main__':
    unittest.main()
