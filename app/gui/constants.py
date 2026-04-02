from __future__ import annotations

COLUNAS_TABELA = [
    ("gtin", "GTIN", 130),
    ("cod_winthor", "Cod. Winthor", 110),
    ("status_sefaz", "Status Sefaz", 110),
    ("ncm_winthor", "NCM Winthor", 110),
    ("ncm_oficial", "NCM GS1", 110),
    ("divergencia_ncm", "Divergencia", 220),
    ("descricao_erp", "Descricao ERP", 240),
    ("descricao_produto", "Descricao GS1", 240),
    ("cest", "CEST", 100),
    ("data_hora_resposta", "Data Resposta", 140),
    ("ultima_atualizacao", "Atualizado Em", 140),
]


COLUNAS_CENARIOS = [
    ("ncm", "NCM", 110),
    ("cst", "CST", 90),
    ("cclasstrib", "cClassTrib", 120),
    ("condicao_legal", "Condicao Legal", 260),
    ("descricao_dossie", "Descricao Dossie", 220),
    ("p_red_ibs", "pRedIBS", 90),
    ("p_red_cbs", "pRedCBS", 90),
    ("publicacao", "Publicacao", 150),
    ("inicio_vigencia", "InicioVigencia", 150),
    ("anexo", "Anexo", 80),
    ("ind_nfe", "IndNFe", 90),
    ("ind_nfce", "IndNFCe", 90),
    ("base_legal", "Base Legal", 220),
    ("fonte", "Fonte", 170),
    ("ultima_atualizacao", "Atualizado Em", 140),
]


COLUNAS_ANEXOS = [
    ("anexo", "Anexo", 90),
    ("descricao", "Descricao Anexo", 220),
    ("publicacao", "Publicacao", 150),
    ("inicio_vigencia", "InicioVigencia", 150),
    ("total_ncms_relacionados", "Qtd. NCMs", 100),
    ("total_cenarios_relacionados", "Qtd. Cenarios", 110),
    ("ncms_relacionados", "NCMs Relacionados", 220),
    ("csts_relacionados", "CSTs Relacionados", 150),
    ("cclasstrib_relacionados", "cClassTribs Rel.", 220),
    ("codigo_especificidade", "Cod. Especificidade", 150),
    ("descr_item_anexo", "descrItemAnexo", 260),
    ("valor", "Valor", 110),
    ("tipo", "Tipo", 110),
    ("especificidade_publicacao", "Pub. Especificidade", 150),
    ("especificidade_inicio_vigencia", "Inicio Vig. Espec.", 150),
    ("ultima_atualizacao", "Atualizado Em", 140),
]


COLUNAS_SELECAO_GTINS = [
    ("codprod", "Cod. Winthor", 140),
    ("gtin", "GTIN", 180),
    ("ncm", "NCM", 160),
    ("descricao_erp", "Descricao ERP", 320),
    ("status_consulta", "Status da Consulta", 280),
]
