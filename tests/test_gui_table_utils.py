from __future__ import annotations

import unittest

from app.gui.table_utils import apply_column_filters, sort_records


class GuiTableUtilsTest(unittest.TestCase):
    def test_apply_column_filters_usa_todas_as_colunas_de_forma_case_insensitive(self) -> None:
        registros = [
            {"gtin": "7891", "descricao": "Campari", "status": "Consultado"},
            {"gtin": "7892", "descricao": "Whisky", "status": "Nao consultado"},
        ]

        filtrados = apply_column_filters(registros, {"descricao": "camp", "status": "consult"})

        self.assertEqual([registro["gtin"] for registro in filtrados], ["7891"])

    def test_sort_records_ordena_numeros_datas_e_texto(self) -> None:
        registros = [
            {"codigo": "20", "data": "01/04/2026 10:00:00", "descricao": "Beta"},
            {"codigo": "3", "data": "31/03/2026 10:00:00", "descricao": "alpha"},
            {"codigo": "", "data": "", "descricao": ""},
        ]

        ordenados_codigo = sort_records(registros, "codigo")
        ordenados_data = sort_records(registros, "data", reverse=True)
        ordenados_descricao = sort_records(registros, "descricao")

        self.assertEqual([item["codigo"] for item in ordenados_codigo], ["3", "20", ""])
        self.assertEqual([item["data"] for item in ordenados_data], ["01/04/2026 10:00:00", "31/03/2026 10:00:00", ""])
        self.assertEqual([item["descricao"] for item in ordenados_descricao], ["alpha", "Beta", ""])


if __name__ == '__main__':
    unittest.main()
