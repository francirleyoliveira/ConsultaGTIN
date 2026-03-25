from __future__ import annotations

from typing import Any, Callable
import re

import requests
from lxml import html

from app.config import Settings


class ModuloBIndisponivelError(RuntimeError):
    """Indica que a consulta do portal nao esta disponivel no ambiente atual."""


class ConformidadeScraperService:
    def __init__(
        self,
        settings: Settings,
        extractor: Callable[[str], list[dict[str, Any]]] | None = None,
    ) -> None:
        self.settings = settings
        self.extractor = extractor

    def buscar_cenarios_por_ncm(self, ncm: str) -> list[dict[str, Any]]:
        ncm_limpo = "".join(filter(str.isdigit, str(ncm or "")))
        if not ncm_limpo:
            return []
        if self.extractor is not None:
            return [self._normalizar_cenario(item, ncm_limpo) for item in self.extractor(ncm_limpo)]

        try:
            return self._buscar_por_http(ncm_limpo)
        except ModuloBIndisponivelError:
            raise
        except Exception:
            return self._buscar_com_selenium(ncm_limpo)

    def _buscar_por_http(self, ncm: str) -> list[dict[str, Any]]:
        try:
            resposta = requests.get(
                self.settings.portal_conformidade_url,
                params={"ncm": ncm},
                timeout=20,
            )
            resposta.raise_for_status()
        except Exception as exc:
            raise ModuloBIndisponivelError(f"Falha na consulta HTTP ao portal de conformidade: {exc}") from exc

        cenarios = self._parse_cenarios_html(resposta.text, ncm)
        if not cenarios:
            raise ModuloBIndisponivelError(
                f"Nenhum cenario foi encontrado para o NCM {ncm} no portal de conformidade."
            )
        return cenarios

    def _buscar_com_selenium(self, ncm: str) -> list[dict[str, Any]]:
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as ec
            from selenium.webdriver.support.ui import WebDriverWait
        except ImportError as exc:
            raise ModuloBIndisponivelError(
                "Selenium nao esta instalado. Adicione 'selenium' ao ambiente para habilitar o modulo B."
            ) from exc

        driver = None
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1440,1200")
            options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(options=options)
            driver.get(self.settings.portal_conformidade_url)

            wait = WebDriverWait(driver, 20)
            campo_ncm = wait.until(ec.presence_of_element_located((By.NAME, "ncm")))
            campo_ncm.clear()
            campo_ncm.send_keys(ncm)
            campo_ncm.submit()

            wait.until(ec.presence_of_element_located((By.ID, "cardsGrid")))
            cenarios = self._parse_cenarios_html(driver.page_source, ncm)
            if not cenarios:
                raise ModuloBIndisponivelError(
                    f"Nenhum cenario foi encontrado para o NCM {ncm} no portal de conformidade."
                )
            return cenarios
        except Exception as exc:
            mensagem = str(exc)
            if "ERR_NAME_NOT_RESOLVED" in mensagem:
                mensagem = (
                    f"Portal de conformidade indisponivel ou URL invalida: {self.settings.portal_conformidade_url}"
                )
            raise ModuloBIndisponivelError(f"Falha no scraping do portal de conformidade: {mensagem}") from exc
        finally:
            if driver is not None:
                driver.quit()

    def _parse_cenarios_html(self, conteudo_html: str, ncm: str) -> list[dict[str, Any]]:
        documento = html.fromstring(conteudo_html)
        cards = documento.xpath("//div[contains(@class, 'result-card')]")
        cenarios: list[dict[str, Any]] = []
        for card in cards:
            cst = (card.get("data-cst") or "").strip()
            cclasstrib = (card.get("data-class-trib") or "").strip()
            codigo_visivel = self._texto_xpath(card, ".//h4[contains(@class, 'card-code')]/text()")
            descricao = self._texto_xpath(card, ".//p[contains(@class, 'card-desc')]//text()")
            condicao = self._texto_xpath(card, ".//p[contains(@class, 'condicao-text')]//text()")
            categoria = self._texto_xpath(card, ".//span[contains(@class, 'card-badge')]//text()")
            if not cclasstrib:
                onclick = card.get("onclick") or ""
                match = re.search(r"cClass=([0-9A-Za-z]+)", onclick)
                if match:
                    cclasstrib = match.group(1)
            if not cclasstrib:
                cclasstrib = codigo_visivel
            if not cclasstrib:
                continue
            cenarios.append(
                self._normalizar_cenario(
                    {
                        "ncm": ncm,
                        "cst": cst,
                        "cclasstrib": cclasstrib,
                        "condicao_legal": condicao or descricao,
                        "fonte": categoria or "portal_conformidade_facil",
                    },
                    ncm,
                )
            )
        return cenarios

    def _texto_xpath(self, node, xpath: str) -> str:
        partes = [str(parte).strip() for parte in node.xpath(xpath) if str(parte).strip()]
        return " ".join(partes)

    def _normalizar_cenario(self, cenario: dict[str, Any], ncm: str) -> dict[str, Any]:
        return {
            "ncm": "".join(filter(str.isdigit, str(cenario.get("ncm", ncm) or ncm))),
            "cst": str(cenario.get("cst", "") or "").strip(),
            "cclasstrib": str(cenario.get("cclasstrib", "") or "").strip(),
            "condicao_legal": str(cenario.get("condicao_legal", "") or "").strip(),
            "fonte": str(cenario.get("fonte", "portal_conformidade_facil") or "portal_conformidade_facil").strip(),
        }
