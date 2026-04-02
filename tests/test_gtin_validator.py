from __future__ import annotations

import unittest

from app.validators.gtin import validar_digito_gtin


class GTINValidatorTest(unittest.TestCase):
    def test_rejeita_tamanhos_fora_do_padrao_gs1(self) -> None:
        self.assertFalse(validar_digito_gtin("123456789012345"))
        self.assertFalse(validar_digito_gtin("1234567890123456"))


if __name__ == "__main__":
    unittest.main()
