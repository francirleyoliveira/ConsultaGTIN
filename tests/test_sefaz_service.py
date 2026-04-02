from __future__ import annotations

import unittest
from unittest.mock import patch

from app.config import Settings
from app.services.sefaz_service import GTIN_XML_VERSAO, montar_envelope_gtin, realizar_healthcheck_gtin


class SefazServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            db_user=None,
            db_pass=None,
            db_dsn=None,
            cert_senha=None,
            cert_caminho=None,
            oracle_client_caminho=None,
            gtin_healthcheck_gtin='7891032015604',
            gtin_healthcheck_timeout_seconds=5,
        )

    def test_envelope_gtin_usa_versao_100_por_padrao(self) -> None:
        envelope = montar_envelope_gtin('7891032015604')
        self.assertEqual('1.00', GTIN_XML_VERSAO)
        self.assertIn('versao="1.00"', envelope)

    @patch('app.services.sefaz_service.consultar_gtin_sefaz')
    def test_healthcheck_aceita_status_949x_como_servico_ativo(self, consultar_mock) -> None:
        consultar_mock.return_value = {
            'status': '9498',
            'motivo': 'Rejeicao: GTIN existe no CCG com NCM invalido',
            'gtin': '7891032015604',
        }

        resultado = realizar_healthcheck_gtin(self.settings)

        self.assertTrue(resultado['ok'])
        self.assertEqual('9498', resultado['status'])

    @patch('app.services.sefaz_service.consultar_gtin_sefaz')
    def test_healthcheck_rejeita_status_239_como_indisponibilidade_de_layout(self, consultar_mock) -> None:
        consultar_mock.return_value = {
            'status': '239',
            'motivo': 'Rejeicao: Versao do arquivo XML nao suportada',
            'gtin': '7891032015604',
        }

        resultado = realizar_healthcheck_gtin(self.settings)

        self.assertFalse(resultado['ok'])
        self.assertEqual('239', resultado['status'])
        self.assertIn('Versao do arquivo XML nao suportada', resultado['motivo'])

    @patch('app.services.sefaz_service.consultar_gtin_sefaz')
    def test_healthcheck_retorna_erro_de_configuracao_quando_gtin_referencia_esta_vazio(self, consultar_mock) -> None:
        settings = Settings(
            db_user=None,
            db_pass=None,
            db_dsn=None,
            cert_senha=None,
            cert_caminho=None,
            oracle_client_caminho=None,
            gtin_healthcheck_gtin='',
        )

        resultado = realizar_healthcheck_gtin(settings)

        self.assertFalse(resultado['ok'])
        self.assertEqual('CONFIG', resultado['status'])
        self.assertIn('nao configurado', resultado['motivo'])
        consultar_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
