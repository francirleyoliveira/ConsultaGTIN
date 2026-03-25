from __future__ import annotations


def parse_positive_int(value: str, default: int = 10, minimum: int = 1, maximum: int = 5000) -> int:
    texto = str(value or '').strip()
    if not texto:
        return default
    try:
        numero = int(texto)
    except ValueError as exc:
        raise ValueError('Informe um numero inteiro valido.') from exc
    if numero < minimum:
        return minimum
    if numero > maximum:
        return maximum
    return numero
