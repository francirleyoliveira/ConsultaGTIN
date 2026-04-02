from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
RELATORIOS_DIR = OUTPUT_DIR / "relatorios"
DIAGNOSTICOS_DIR = OUTPUT_DIR / "diagnosticos"
DATA_DIR = BASE_DIR / "data"
QUERIES_DIR = Path(__file__).resolve().parent / "queries"
SQLITE_DB_PATH = DATA_DIR / "consulta_gtin.db"
PORTAL_CONFORMIDADE_PADRAO = "https://dfe-portal.svrs.rs.gov.br/CFF/ClassificacaoTributariaNCM"
CFF_WSDL_PADRAO = "https://cff.svrs.rs.gov.br/api/v1/consultas/classTrib?wsdl"
CFF_API_PADRAO = "https://cff.svrs.rs.gov.br/api/v1/consultas/classTrib"
CFF_ANEXOS_API_PADRAO = "https://cff.svrs.rs.gov.br/api/v1/consultas/anexos"


def _parse_bool_env(nome: str, padrao: bool) -> bool:
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() not in {"0", "false", "no", "nao", "não", "off"}


@dataclass(frozen=True)
class Settings:
    db_user: str | None
    db_pass: str | None
    db_dsn: str | None
    cert_senha: str | None
    cert_caminho: str | None
    oracle_client_caminho: str | None
    url_webservice: str = "https://dfe-servico.svrs.rs.gov.br/ws/ccgConsGTIN/ccgConsGTIN.asmx"
    portal_conformidade_url: str = PORTAL_CONFORMIDADE_PADRAO
    cff_wsdl_url: str = CFF_WSDL_PADRAO
    cff_api_url: str = CFF_API_PADRAO
    cff_anexos_api_url: str = CFF_ANEXOS_API_PADRAO
    cff_resposta_exemplo_path: str | None = None
    cff_anexos_resposta_exemplo_path: str | None = None
    gtin_healthcheck_gtin: str = "7891032015604"
    gtin_healthcheck_timeout_seconds: int = 12
    gtin_healthcheck_ttl_seconds: int = 300
    gtin_circuit_breaker_seconds: int = 180
    gtin_circuit_breaker_failures: int = 2
    ssl_verify: bool = True
    ssl_ca_bundle: str | None = None
    ai_enabled: bool = True
    ai_provider: str = "heuristic"
    ai_model: str = "tax-scenario-heuristic-v1"
    ai_prompt_version: str = "tax-scenario-v1"

    @property
    def requests_verify(self) -> bool | str:
        return self.ssl_ca_bundle or self.ssl_verify



def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        db_user=os.getenv("DB_USER"),
        db_pass=os.getenv("DB_PASS"),
        db_dsn=os.getenv("DB_DSN"),
        cert_senha=os.getenv("CERT_SENHA"),
        cert_caminho=os.getenv("CERT_CAMINHO"),
        oracle_client_caminho=os.getenv("ORACLE_CLIENT_CAMINHO"),
        portal_conformidade_url=os.getenv("PORTAL_CONFORMIDADE_URL", PORTAL_CONFORMIDADE_PADRAO),
        cff_wsdl_url=os.getenv("CFF_WSDL_URL", CFF_WSDL_PADRAO),
        cff_api_url=os.getenv("CFF_API_URL", CFF_API_PADRAO),
        cff_anexos_api_url=os.getenv("CFF_ANEXOS_API_URL", CFF_ANEXOS_API_PADRAO),
        cff_resposta_exemplo_path=os.getenv("CFF_RESPOSTA_EXEMPLO_PATH"),
        cff_anexos_resposta_exemplo_path=os.getenv("CFF_ANEXOS_RESPOSTA_EXEMPLO_PATH"),
        gtin_healthcheck_gtin=os.getenv("GTIN_HEALTHCHECK_GTIN", "7891032015604"),
        gtin_healthcheck_timeout_seconds=int(os.getenv("GTIN_HEALTHCHECK_TIMEOUT_SECONDS", "12")),
        gtin_healthcheck_ttl_seconds=int(os.getenv("GTIN_HEALTHCHECK_TTL_SECONDS", "300")),
        gtin_circuit_breaker_seconds=int(os.getenv("GTIN_CIRCUIT_BREAKER_SECONDS", "180")),
        gtin_circuit_breaker_failures=int(os.getenv("GTIN_CIRCUIT_BREAKER_FAILURES", "2")),
        ssl_verify=_parse_bool_env("SSL_VERIFY", True),
        ssl_ca_bundle=os.getenv("SSL_CA_BUNDLE") or None,
        ai_enabled=_parse_bool_env("AI_ENABLED", True),
        ai_provider=os.getenv("AI_PROVIDER", "heuristic"),
        ai_model=os.getenv("AI_MODEL", "tax-scenario-heuristic-v1"),
        ai_prompt_version=os.getenv("AI_PROMPT_VERSION", "tax-scenario-v1"),
    )



def ensure_output_dirs() -> None:
    RELATORIOS_DIR.mkdir(parents=True, exist_ok=True)
    DIAGNOSTICOS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
