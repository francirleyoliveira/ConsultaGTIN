from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from app.config import Settings
from app.services.gtin_health_service import GtinServiceHealthMonitor


class FakeClock:
    def __init__(self, initial: datetime) -> None:
        self.current = initial

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


class GtinServiceHealthMonitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            db_user=None,
            db_pass=None,
            db_dsn=None,
            cert_senha=None,
            cert_caminho=None,
            oracle_client_caminho=None,
            gtin_healthcheck_ttl_seconds=300,
            gtin_circuit_breaker_seconds=120,
            gtin_circuit_breaker_failures=2,
        )
        self.clock = FakeClock(datetime(2026, 3, 25, 20, 0, 0))

    def test_reutiliza_cache_de_healthcheck_saudavel(self) -> None:
        chamadas = [0]

        def checker(*_args, **_kwargs):
            chamadas[0] += 1
            return {
                "ok": True,
                "status": "9498",
                "motivo": "Servico ativo",
                "gtin_teste": "7891032015604",
            }

        monitor = GtinServiceHealthMonitor(self.settings, checker=checker, now_provider=self.clock)

        primeiro = monitor.verificar()
        segundo = monitor.verificar()

        self.assertTrue(primeiro["ok"])
        self.assertFalse(primeiro["from_cache"])
        self.assertTrue(segundo["ok"])
        self.assertTrue(segundo["from_cache"])
        self.assertEqual(1, chamadas[0])

    def test_abre_circuit_breaker_apos_falhas_consecutivas(self) -> None:
        chamadas = [0]

        def checker(*_args, **_kwargs):
            chamadas[0] += 1
            return {
                "ok": False,
                "status": "Erro",
                "motivo": "HTTP 404",
                "gtin_teste": "7891032015604",
            }

        monitor = GtinServiceHealthMonitor(self.settings, checker=checker, now_provider=self.clock)

        primeiro = monitor.verificar(force=True)
        segundo = monitor.verificar(force=True)
        terceiro = monitor.verificar()

        self.assertFalse(primeiro["ok"])
        self.assertFalse(segundo["ok"])
        self.assertTrue(segundo["blocked"])
        self.assertTrue(terceiro["blocked"])
        self.assertEqual(2, chamadas[0])

        self.clock.advance(121)
        quarto = monitor.verificar(force=True)
        self.assertEqual(3, chamadas[0])
        self.assertFalse(quarto["from_cache"])

    def test_sucesso_de_consulta_real_reseta_circuito(self) -> None:
        monitor = GtinServiceHealthMonitor(
            self.settings,
            checker=lambda *_args, **_kwargs: {
                "ok": False,
                "status": "Erro",
                "motivo": "HTTP 404",
                "gtin_teste": "7891032015604",
            },
            now_provider=self.clock,
        )

        monitor.verificar(force=True)
        monitor.verificar(force=True)
        self.assertTrue(monitor.esta_bloqueado())

        monitor.registrar_resultado_consulta({"status": "9490", "gtin": "7891032015604", "motivo": "Consulta realizada"})

        self.assertFalse(monitor.esta_bloqueado())
        self.assertTrue(monitor.last_result["ok"])

    def test_status_239_nao_reseta_circuito_nem_marca_servico_como_saudavel(self) -> None:
        monitor = GtinServiceHealthMonitor(
            self.settings,
            checker=lambda *_args, **_kwargs: {
                "ok": False,
                "status": "Erro",
                "motivo": "HTTP 404",
                "gtin_teste": "7891032015604",
            },
            now_provider=self.clock,
        )

        monitor.verificar(force=True)
        monitor.verificar(force=True)
        self.assertTrue(monitor.esta_bloqueado())

        monitor.registrar_resultado_consulta(
            {
                "status": "239",
                "gtin": "7891032015604",
                "motivo": "Rejeicao: Versao do arquivo XML nao suportada",
            }
        )

        self.assertTrue(monitor.esta_bloqueado())
        self.assertFalse(monitor.last_result["ok"])
        self.assertEqual("HTTP 404", monitor.last_result["motivo"])


if __name__ == "__main__":
    unittest.main()
