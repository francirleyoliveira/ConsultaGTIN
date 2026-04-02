from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from app.config import Settings
from app.services.anexo_tributario_service import AnexoTributarioService


class AnexoTributarioServiceTest(unittest.TestCase):
    def test_normaliza_anexos_e_especificidades(self) -> None:
        raw = [
            {
                "Anexo": "IV",
                "DescricaoAnexo": "Anexo IV",
                "Publicacao": "2026-03-24T00:00:00",
                "InicioVigencia": "2026-04-01T00:00:00",
                "Especificidades": [
                    {
                        "CodigoEspecificidade": "ESP2",
                        "DescricaoEspecificidade": "Faixa especial",
                        "Valor": "1",
                        "Tipo": "numero",
                        "Publicacao": "2026-03-24T00:00:00",
                        "InicioVigencia": "2026-04-01T00:00:00",
                    }
                ],
            }
        ]
        service = AnexoTributarioService(Settings(None, None, None, None, None, None), fetcher=lambda: raw)

        anexos = service.sincronizar_anexos()

        self.assertEqual(1, len(anexos))
        self.assertEqual("IV", anexos[0]["anexo"])
        self.assertEqual("Anexo IV", anexos[0]["descricao"])
        self.assertEqual(1, len(anexos[0]["especificidades"]))
        self.assertEqual("ESP2", anexos[0]["especificidades"][0]["codigo"])

    def test_normaliza_formato_flat_do_servico(self) -> None:
        raw = [
            {
                "nroAnexo": 91031,
                "codNcmNbs": "105049000",
                "TipoNomenclatura": "NBS",
                "descrCondicao": None,
                "descrExcecao": None,
                "texObservacao": "Transporte",
                "dthIniVig": "2026-01-01T00:00:00",
                "dthFimVig": None,
                "descrItemAnexo": "Servico de transporte multimodal",
                "descrAnexo": "Servicos de transporte",
            }
        ]
        service = AnexoTributarioService(Settings(None, None, None, None, None, None), fetcher=lambda: raw)

        anexos = service.sincronizar_anexos()

        self.assertEqual(1, len(anexos))
        self.assertEqual("91031", anexos[0]["anexo"])
        self.assertEqual("Servicos de transporte", anexos[0]["descricao"])
        self.assertEqual(1, len(anexos[0]["especificidades"]))
        self.assertEqual("105049000", anexos[0]["especificidades"][0]["codigo"])
        self.assertEqual("Servico de transporte multimodal", anexos[0]["especificidades"][0]["descricao"])
        self.assertEqual("Servico de transporte multimodal", anexos[0]["especificidades"][0]["descr_item_anexo"])
        self.assertEqual("Transporte", anexos[0]["especificidades"][0]["valor"])

    @patch('app.services.anexo_tributario_service.get')
    def test_consulta_api_respeita_configuracao_ssl(self, get_mock) -> None:
        resposta = Mock()
        resposta.raise_for_status.return_value = None
        resposta.json.return_value = []
        get_mock.return_value = resposta
        settings = Settings(None, None, None, None, None, None, ssl_ca_bundle='C:/certs/ca.pem')

        service = AnexoTributarioService(settings)
        service.sincronizar_anexos()

        self.assertEqual('C:/certs/ca.pem', get_mock.call_args.kwargs['verify'])


if __name__ == "__main__":
    unittest.main()
