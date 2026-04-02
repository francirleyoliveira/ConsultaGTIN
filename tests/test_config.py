from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from app.config import load_settings


class ConfigTest(unittest.TestCase):
    @patch.dict(os.environ, {'GTIN_HEALTHCHECK_TIMEOUT_SECONDS': 'abc'}, clear=False)
    def test_load_settings_gera_erro_claro_para_inteiro_invalido(self) -> None:
        with self.assertRaisesRegex(ValueError, 'GTIN_HEALTHCHECK_TIMEOUT_SECONDS'):
            load_settings()


if __name__ == '__main__':
    unittest.main()
