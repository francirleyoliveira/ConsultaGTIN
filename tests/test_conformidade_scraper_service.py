from __future__ import annotations

import unittest

from app.config import Settings
from app.services.conformidade_scraper_service import ConformidadeScraperService


class ConformidadeScraperServiceTest(unittest.TestCase):
    def test_parse_html_extrai_cst_e_cclasstrib(self) -> None:
        html_texto = """
        <div id='cardsGrid'>
            <div class='result-card group card-blue' data-cst='000' data-class-trib='000001'>
                <span class='card-badge badge-gray'>Regra Geral</span>
                <h4 class='card-code'>000001</h4>
                <p class='card-desc'>Situacoes tributadas integralmente pelo IBS e CBS.</p>
            </div>
            <div class='result-card group card-purple' data-cst='200' onclick="location.href='/CFF/ClassificacaoTributaria?cClass=200020'">
                <span class='card-badge badge-gray'>Regra Geral</span>
                <h4 class='card-code'>200020</h4>
                <p class='card-desc'>Operacao praticada por sociedades cooperativas.</p>
                <div class='condicao-box'><p class='condicao-text'><strong>Condição:</strong> COOPERATIVA</p></div>
            </div>
        </div>
        """
        service = ConformidadeScraperService(
            Settings(None, None, None, None, None, None),
            extractor=lambda _ncm: [],
        )

        cenarios = service._parse_cenarios_html(html_texto, "18063220")

        self.assertEqual(2, len(cenarios))
        self.assertEqual("000", cenarios[0]["cst"])
        self.assertEqual("000001", cenarios[0]["cclasstrib"])
        self.assertEqual("200", cenarios[1]["cst"])
        self.assertEqual("200020", cenarios[1]["cclasstrib"])
        self.assertIn("COOPERATIVA", cenarios[1]["condicao_legal"])


if __name__ == "__main__":
    unittest.main()
