def validar_digito_gtin(gtin: str | int | None) -> bool:
    """Valida o digito verificador de um GTIN (8, 12, 13 ou 14 digitos)."""
    try:
        gtin_limpo = str(gtin).strip()
        if not gtin_limpo.isdigit():
            return False
        if len(gtin_limpo) not in {8, 12, 13, 14}:
            return False

        gtin = gtin_limpo.zfill(14)
        corpo = gtin[:-1]
        digito_informado = int(gtin[-1])

        soma = 0
        for i, digito in enumerate(reversed(corpo)):
            peso = 3 if i % 2 == 0 else 1
            soma += int(digito) * peso

        digito_calculado = (10 - (soma % 10)) % 10
        return digito_informado == digito_calculado
    except (TypeError, ValueError):
        return False


def validar_prefixo_gs1_brasil(gtin: str) -> bool:
    gtin_limpo = "".join(filter(str.isdigit, str(gtin or "")))
    return gtin_limpo.startswith(("789", "790"))


def comparar_ncm(ncm_erp: str, ncm_sefaz: str) -> str:
    """
    Compara o NCM do ERP com o retornado pela SEFAZ/GS1.
    Retorna a mensagem de divergencia ou 'OK'.
    """
    erp_limpo = "".join(filter(str.isdigit, str(ncm_erp or "")))
    sefaz_limpo = "".join(filter(str.isdigit, str(ncm_sefaz or "")))

    if not sefaz_limpo:
        return "NCM NAO INFORMADO NA GS1"

    erp_norm = erp_limpo.zfill(8)
    sefaz_norm = sefaz_limpo.zfill(8)

    if erp_norm == sefaz_norm:
        return "OK"

    if len(erp_limpo) < 8:
        return f"DIVERGENTE: ERP INCOMPLETO ({erp_limpo}) != GS1({sefaz_limpo})"

    return f"DIVERGENTE: ERP({erp_limpo}) != GS1({sefaz_limpo})"

