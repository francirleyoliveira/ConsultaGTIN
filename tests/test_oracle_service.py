from __future__ import annotations

import unittest
from unittest.mock import patch

from app.config import Settings
from app.services import oracle_service


class _FakeCursor:
    def __init__(self, rows=None, execute_error: Exception | None = None) -> None:
        self.rows = rows or []
        self.execute_error = execute_error
        self.closed = False
        self.executed_sql = None

    def execute(self, sql: str) -> None:
        self.executed_sql = sql
        if self.execute_error is not None:
            raise self.execute_error

    def fetchall(self):
        return self.rows

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class OracleServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        oracle_service._oracle_client_iniciado = False
        oracle_service._oracle_client_inicializacao_falhou = False
        self.settings = Settings(
            db_user='user',
            db_pass='pass',
            db_dsn='dsn',
            cert_senha=None,
            cert_caminho=None,
            oracle_client_caminho='C:/oracle',
        )

    def tearDown(self) -> None:
        oracle_service._oracle_client_iniciado = False
        oracle_service._oracle_client_inicializacao_falhou = False

    @patch('app.services.oracle_service.oracledb.init_oracle_client', side_effect=RuntimeError('falha'))
    def test_inicializacao_falha_nao_reitera_no_mesmo_processo(self, init_mock) -> None:
        oracle_service.inicializar_oracle_client(self.settings)
        oracle_service.inicializar_oracle_client(self.settings)

        self.assertEqual(1, init_mock.call_count)
        self.assertFalse(oracle_service._oracle_client_iniciado)
        self.assertTrue(oracle_service._oracle_client_inicializacao_falhou)

    @patch('app.services.oracle_service._carregar_sql_consulta', return_value='SELECT 1 FROM dual')
    @patch('app.services.oracle_service.inicializar_oracle_client')
    @patch('app.services.oracle_service.oracledb.connect')
    def test_busca_gtins_fecha_recursos_mesmo_com_erro(self, connect_mock, _init_mock, _sql_mock) -> None:
        cursor = _FakeCursor(execute_error=RuntimeError('db error'))
        conexao = _FakeConnection(cursor)
        connect_mock.return_value = conexao

        resultado = oracle_service.buscar_gtins_winthor(self.settings)

        self.assertEqual([], resultado)
        self.assertTrue(cursor.closed)
        self.assertTrue(conexao.closed)

    @patch('app.services.oracle_service._carregar_sql_consulta', return_value='SELECT 1 FROM dual')
    @patch('app.services.oracle_service.inicializar_oracle_client')
    @patch('app.services.oracle_service.oracledb.connect')
    def test_busca_gtins_retorna_linhas_em_caso_de_sucesso(self, connect_mock, _init_mock, _sql_mock) -> None:
        cursor = _FakeCursor(rows=[('1', '7891234567895', '22030000')])
        conexao = _FakeConnection(cursor)
        connect_mock.return_value = conexao

        resultado = oracle_service.buscar_gtins_winthor(self.settings)

        self.assertEqual([('1', '7891234567895', '22030000')], resultado)
        self.assertTrue(cursor.closed)
        self.assertTrue(conexao.closed)


if __name__ == '__main__':
    unittest.main()
