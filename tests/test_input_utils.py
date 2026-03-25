from __future__ import annotations

import unittest

from app.utils.input_utils import parse_positive_int


class InputUtilsTest(unittest.TestCase):
    def test_parse_positive_int_limita_faixa(self) -> None:
        self.assertEqual(1, parse_positive_int('0', default=10, minimum=1, maximum=5000))
        self.assertEqual(5000, parse_positive_int('99999', default=10, minimum=1, maximum=5000))
        self.assertEqual(25, parse_positive_int('25', default=10, minimum=1, maximum=5000))

    def test_parse_positive_int_rejeita_texto_invalido(self) -> None:
        with self.assertRaises(ValueError):
            parse_positive_int('abc', default=10, minimum=1, maximum=5000)


if __name__ == '__main__':
    unittest.main()
