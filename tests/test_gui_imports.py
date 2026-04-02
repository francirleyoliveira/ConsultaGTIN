from __future__ import annotations

import unittest

from app.gui.app_gtin import AppGTIN as AppGTINDireto
from app.gui.interface import AppGTIN, AnaliseIAWindow, AnexosTributariosWindow, CenariosTributariosWindow, ConsultaDatabaseWindow, SelecaoGtinsWindow
from app.gui.mixins.async_table_window import AsyncTableWindowMixin


class GuiImportTest(unittest.TestCase):
    def test_interface_reexporta_classes_principais(self) -> None:
        self.assertIs(AppGTIN, AppGTINDireto)
        self.assertTrue(issubclass(ConsultaDatabaseWindow, AsyncTableWindowMixin))
        self.assertTrue(issubclass(CenariosTributariosWindow, AsyncTableWindowMixin))
        self.assertTrue(issubclass(AnexosTributariosWindow, AsyncTableWindowMixin))
        self.assertTrue(callable(SelecaoGtinsWindow))
        self.assertTrue(callable(AnaliseIAWindow))


if __name__ == '__main__':
    unittest.main()
