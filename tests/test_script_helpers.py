from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime
from unittest.mock import Mock

from app.config import Settings
from scripts.revalidar_ncm_sqlite import revalidar_ncms_conn
from scripts.teste_webservice import executar_teste_webservice
from scripts.ver_resposta_bruta import baixar_resposta_bruta


class ScriptHelpersTest(unittest.TestCase):
    def test_revalidar_ncms_conn_atualiza_timestamps_e_ordem(self) -> None:
        conn = sqlite3.connect(':memory:')
        conn.execute(
            """
            CREATE TABLE consultas_gtin (
                gtin TEXT PRIMARY KEY,
                ncm_winthor TEXT,
                ncm_oficial TEXT,
                status_sefaz TEXT,
                divergencia_ncm TEXT,
                ultima_atualizacao TEXT,
                ultima_atualizacao_ordem TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO consultas_gtin (
                gtin, ncm_winthor, ncm_oficial, status_sefaz, divergencia_ncm, ultima_atualizacao, ultima_atualizacao_ordem
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ('7891234567895', '22030000', '22039900', '9490', 'OK', '', ''),
        )

        total, atualizados = revalidar_ncms_conn(conn, now=datetime(2026, 4, 2, 10, 30, 0))
        row = conn.execute('SELECT divergencia_ncm, ultima_atualizacao, ultima_atualizacao_ordem FROM consultas_gtin').fetchone()

        self.assertEqual(1, total)
        self.assertEqual(1, atualizados)
        self.assertEqual('DIVERGENTE: ERP(22030000) != GS1(22039900)', row[0])
        self.assertEqual('02/04/2026 10:30:00', row[1])
        self.assertEqual('2026-04-02 10:30:00', row[2])

    def test_teste_webservice_respeita_requests_verify(self) -> None:
        settings = Settings(None, None, None, 'senha', 'cert.pfx', None, ssl_verify=False)
        resposta = Mock()
        fake_post = Mock(return_value=resposta)

        resultado = executar_teste_webservice(settings=settings, post_func=fake_post)

        self.assertIs(resultado, resposta)
        self.assertFalse(fake_post.call_args.kwargs['verify'])

    def test_ver_resposta_bruta_respeita_ca_bundle(self) -> None:
        settings = Settings(None, None, None, 'senha', 'cert.pfx', None, ssl_ca_bundle='ca-bundle.pem')
        resposta = Mock()
        fake_post = Mock(return_value=resposta)

        resultado = baixar_resposta_bruta(settings=settings, post_func=fake_post)

        self.assertIs(resultado, resposta)
        self.assertEqual('ca-bundle.pem', fake_post.call_args.kwargs['verify'])


if __name__ == '__main__':
    unittest.main()
