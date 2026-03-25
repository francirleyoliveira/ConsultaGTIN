PRD (Product Requirements Document)
Nome do Projeto: Assistente para Classificacao Tributaria
Versao: 2.1
Data de Atualizacao: 25/03/2026

1. Visao do Produto
O Assistente para Classificacao Tributaria e uma aplicacao desktop em Python voltada para operacao fiscal e suporte a ERP. O produto cruza GTIN, NCM, CST, cClassTrib, dossie tributario e anexos legais, consolidando tudo em uma base SQLite local com filtros, consulta manual, atualizacao incremental e exportacao.

2. Problema que o Produto Resolve
Hoje a classificacao tributaria exige consulta manual em varias fontes: ERP, SEFAZ, Portal Conformidade Facil e servicos do CFF/SVRS. Isso aumenta tempo operacional, risco de erro fiscal, retrabalho em lote e dificulta auditoria.

3. Objetivos do Produto
- Consultar GTIN na SEFAZ e comparar o NCM oficial com o NCM do ERP.
- Permitir classificacao tributaria por GTIN ou diretamente por NCM vindo do ERP.
- Mapear todos os cenarios tributarios aplicaveis a um NCM.
- Enriquecer os cenarios com dados do catalogo tributario e do dossie por cClassTrib.
- Sincronizar anexos e especificidades do servico dedicado.
- Persistir tudo em SQLite para reuso, auditoria local, filtros e exportacao.

4. Publico-Alvo
- Analistas fiscais
- Times de sustentacao ERP
- Programadores de integracao fiscal
- Usuarios que preparam emissao de NF-e, NFC-e e documentos relacionados

5. Escopo Funcional Atual
5.1 Entrada e Selecao de Dados
- Selecionar GTINs manualmente a partir da base Oracle/Winthor.
- Rodar processamento automatico por quantidade limitada.
- Consultar NCMs diretamente do ERP, sem depender da etapa de GTIN.

5.2 Consulta GTIN
- Validar digito verificador do GTIN.
- Validar prefixo GS1 Brasil.
- Consultar a SEFAZ SVRS via certificado digital.
- Registrar status, motivo, NCM oficial, descricao e CEST.
- Comparar NCM ERP x NCM oficial e registrar divergencia.

5.3 Classificacao Tributaria por NCM
- Consultar cenarios tributarios por NCM no Portal Conformidade Facil.
- Persistir CST, cClassTrib, condicao legal e fonte.
- Permitir consulta por GTIN ou diretamente por NCM.

5.4 Dossie Tributario
- Sincronizar catalogo tributario autenticado por certificado digital.
- Persistir CSTs e dossies por cClassTrib.
- Enriquecer cenarios com descricao do dossie, pRedIBS, pRedCBS, publicacao, inicio de vigencia, anexo, permissoes de emissao e base legal.

5.5 Anexos e Especificidades
- Sincronizar anexos a partir do servico dedicado.
- Persistir anexos em tabela separada e especificidades em tabela filha.
- Exibir janela propria de anexos com filtros por anexo, descricao, codigo de especificidade, descricao da especificidade e tipo.
- Permitir atualizacao manual dos anexos via botao "Atualizar do Servico".

5.6 Persistencia e Consulta Local
- Manter historico local em SQLite.
- Atualizar registros existentes quando GTIN, NCM, cClassTrib ou anexos forem sincronizados novamente.
- Permitir filtros, revalidacao e exportacao para planilha.

5.7 Interface Operacional
- Janela principal com log operacional, barra de progresso e cards de resumo.
- Janela de consultas GTIN persistidas.
- Janela de cenarios tributarios por NCM.
- Janela de anexos e especificidades.
- Janela de selecao manual de GTINs com status de consulta ja realizada.

6. Fluxos Principais do Usuario
Fluxo A: GTIN -> SEFAZ -> NCM -> Portal -> Dossie -> SQLite.
Fluxo B: ERP -> NCM -> Portal -> Dossie -> SQLite.
Fluxo C: Servico de Anexos -> SQLite -> Janela de Anexos.
Fluxo D: SQLite -> Filtros -> Exportacao em planilha.

7. Requisitos Nao Funcionais
- Aplicacao desktop local com GUI em CustomTkinter.
- Persistencia local em SQLite, sem dependencia de servidor de aplicacao.
- Integracao com Oracle/Winthor e servicos autenticados por certificado A1.
- Operacoes longas executadas em threads para manter a interface responsiva.
- Exportacao simples para Excel para apoio operacional.
- Atualizacao incremental e preservacao de historico local.

8. Indicadores de Sucesso
- Reducao do tempo de consulta fiscal por item.
- Reducao de divergencias manuais de NCM e CST.
- Reuso de dados consultados sem necessidade de consultar tudo novamente.
- Base local capaz de sustentar analise, filtro e exportacao para auditoria.

9. Fora de Escopo Atual
- Multiusuario com sincronizacao remota.
- API publica para terceiros.
- Workflow de aprovacao fiscal.
- Controle de usuarios e perfis.
- Governanca centralizada de certificados.

10. Pendencias Tecnicas Conhecidas
- Endurecimento de seguranca das chamadas com certificado e SSL.
- Melhor tratamento de erros Oracle e SEFAZ.
- Otimizacao do fallback Selenium para lotes maiores.
- Preparacao explicita para empacotamento final do aplicativo.
