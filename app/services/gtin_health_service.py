from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

from app.config import Settings
from app.services.sefaz_service import realizar_healthcheck_gtin


class GtinServiceHealthMonitor:
    def __init__(
        self,
        settings: Settings,
        checker: Callable[[Settings, str | None, int | float | None], dict[str, Any]] = realizar_healthcheck_gtin,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.checker = checker
        self.now_provider = now_provider or datetime.now
        self.last_check_at: datetime | None = None
        self.last_result: dict[str, Any] | None = None
        self.consecutive_failures = 0
        self.circuit_open_until: datetime | None = None

    def verificar(self, force: bool = False) -> dict[str, Any]:
        agora = self.now_provider()
        if self._circuito_aberto(agora):
            resultado = self._resultado_circuito(agora)
            self.last_result = resultado
            return resultado

        ttl = max(int(self.settings.gtin_healthcheck_ttl_seconds), 0)
        if (
            not force
            and ttl > 0
            and self.last_result
            and self.last_result.get("ok")
            and self.last_check_at
            and (agora - self.last_check_at).total_seconds() < ttl
        ):
            cached = dict(self.last_result)
            cached["from_cache"] = True
            cached["message"] = self._formatar_mensagem(cached, cache=True)
            return cached

        bruto = dict(
            self.checker(
                self.settings,
                self.settings.gtin_healthcheck_gtin,
                self.settings.gtin_healthcheck_timeout_seconds,
            )
        )
        resultado = self._normalizar_resultado(bruto, agora, from_cache=False)
        self.last_check_at = agora
        self.last_result = resultado
        return resultado

    def registrar_resultado_consulta(self, consulta: dict[str, Any]) -> None:
        agora = self.now_provider()
        status = str(consulta.get("status") or consulta.get("status_sefaz") or "").strip()
        if not status or status in {"GTIN_INVALIDO", "GTIN_FORA_GS1_BR", "SEFAZ_INDISPONIVEL"}:
            return

        if status.startswith("949"):
            self.consecutive_failures = 0
            self.circuit_open_until = None
            self.last_check_at = agora
            self.last_result = {
                "ok": True,
                "status": status,
                "motivo": str(consulta.get("motivo") or consulta.get("motivo_sefaz") or "").strip(),
                "gtin_teste": str(consulta.get("gtin") or self.settings.gtin_healthcheck_gtin),
                "checked_at": agora.strftime("%d/%m/%Y %H:%M:%S"),
                "from_cache": False,
                "blocked": False,
                "message": f"Servico GTIN respondeu normalmente com status {status}.",
            }
            return

        if status == "Erro":
            motivo = str(consulta.get("motivo") or consulta.get("motivo_sefaz") or "Falha no transporte com a Sefaz").strip()
            self.last_check_at = agora
            self.last_result = self._normalizar_resultado(
                {
                    "ok": False,
                    "status": status,
                    "motivo": motivo,
                    "gtin_teste": str(consulta.get("gtin") or self.settings.gtin_healthcheck_gtin),
                },
                agora,
                from_cache=False,
            )

    def esta_bloqueado(self) -> bool:
        return self._circuito_aberto(self.now_provider())

    def _circuito_aberto(self, agora: datetime) -> bool:
        return self.circuit_open_until is not None and agora < self.circuit_open_until

    def _resultado_circuito(self, agora: datetime) -> dict[str, Any]:
        motivo_base = ""
        if self.last_result:
            motivo_base = str(self.last_result.get("motivo") or "").strip()
        texto_ate = self.circuit_open_until.strftime("%H:%M:%S") if self.circuit_open_until else ""
        motivo = motivo_base or "falhas consecutivas de comunicacao com a Sefaz"
        return {
            "ok": False,
            "status": "SEFAZ_INDISPONIVEL",
            "motivo": motivo,
            "gtin_teste": self.settings.gtin_healthcheck_gtin,
            "checked_at": agora.strftime("%d/%m/%Y %H:%M:%S"),
            "from_cache": False,
            "blocked": True,
            "message": (
                f"Circuit breaker GTIN ativo ate {texto_ate} apos {self.consecutive_failures} falha(s) consecutiva(s): {motivo}."
            ),
        }

    def _normalizar_resultado(self, bruto: dict[str, Any], agora: datetime, from_cache: bool) -> dict[str, Any]:
        ok = bool(bruto.get("ok"))
        if ok:
            self.consecutive_failures = 0
            self.circuit_open_until = None
        else:
            self.consecutive_failures += 1
            if self.consecutive_failures >= max(int(self.settings.gtin_circuit_breaker_failures), 1):
                self.circuit_open_until = agora + timedelta(seconds=max(int(self.settings.gtin_circuit_breaker_seconds), 1))
        resultado = {
            "ok": ok,
            "status": str(bruto.get("status") or ""),
            "motivo": str(bruto.get("motivo") or "").strip(),
            "gtin_teste": str(bruto.get("gtin_teste") or self.settings.gtin_healthcheck_gtin),
            "checked_at": bruto.get("checked_at") or agora.strftime("%d/%m/%Y %H:%M:%S"),
            "from_cache": from_cache,
            "blocked": self._circuito_aberto(agora),
        }
        resultado["message"] = self._formatar_mensagem(resultado, cache=from_cache)
        return resultado

    def _formatar_mensagem(self, resultado: dict[str, Any], cache: bool) -> str:
        if resultado.get("ok"):
            prefixo = "Preflight GTIN reutilizado do cache" if cache else "Preflight GTIN OK"
            status = str(resultado.get("status") or "").strip()
            gtin_teste = str(resultado.get("gtin_teste") or self.settings.gtin_healthcheck_gtin)
            return f"{prefixo}: servico respondeu com status {status or 'n/d'} para o GTIN de referencia {gtin_teste}."

        if resultado.get("blocked"):
            texto_ate = self.circuit_open_until.strftime("%H:%M:%S") if self.circuit_open_until else ""
            return (
                f"Circuit breaker GTIN ativo ate {texto_ate} apos {self.consecutive_failures} falha(s) consecutiva(s): "
                f"{resultado.get('motivo') or 'Falha no transporte com a Sefaz'}."
            )

        return f"Preflight GTIN falhou: {resultado.get('motivo') or 'Falha no transporte com a Sefaz'}."
