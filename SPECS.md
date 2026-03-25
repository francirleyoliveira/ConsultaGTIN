Especificacoes Tecnicas (Tech Specs)
Nome do Projeto: Assistente para Classificacao Tributaria
Versao: 2.1
Data de Atualizacao: 25/03/2026

1. Arquitetura Atual
O sistema segue uma arquitetura desktop local, com GUI em CustomTkinter, servicos de integracao isolados, persistencia em SQLite e processamento assicrono via threads para nao bloquear a interface.

Camadas principais:
- Entrada: `app/main.py`
- Interface: `app/gui/interface.py`
- Orquestracao fiscal: `app/services/classificacao_tributaria_service.py`
- Integracoes externas: Oracle, SEFAZ, Portal Conformidade, catalogo tributario e anexos
- Persistencia local: `app/services/sqlite_service.py`
- Parsers e validadores: `app/parsers/sefaz_xml.py` e `app/validators/gtin.py`
- Utilitarios: `app/utils/input_utils.py`

2. Estrutura de Diretorios Relevantes
- `app/config.py`: configuracao central, paths e variaveis de ambiente
- `app/gui/interface.py`: janela principal e janelas auxiliares
- `app/services/oracle_service.py`: leitura do ERP/Winthor via Oracle
- `app/services/sefaz_service.py`: consulta SOAP do GTIN na SEFAZ
- `app/services/conformidade_scraper_service.py`: consulta de cenarios por NCM no portal
- `app/services/dossie_tributario_service.py`: sincronizacao do catalogo tributario autenticado por certificado
- `app/services/anexo_tributario_service.py`: sincronizacao de anexos e especificidades
- `app/services/sqlite_service.py`: schema, migracoes, consultas e persistencia SQLite
- `app/services/relatorio_service.py`: exportacao para Excel
- `app/queries/consulta_gtins.sql`: SQL base do ERP
- `data/consulta_gtin.db`: banco SQLite local
- `output/relatorios`: saidas Excel
- `output/diagnosticos`: arquivos de apoio tecnico
- `tests/`: testes automatizados dos servicos e utilitarios

3. Stack Tecnologica
Categoria | Tecnologia | Papel Atual
Python 3.x | Runtime principal | Aplicacao desktop e servicos
CustomTkinter | GUI | Interface principal e janelas auxiliares
oracledb | Banco Oracle | Consulta de GTIN/NCM do Winthor
requests-pkcs12 | HTTP com certificado A1 | SEFAZ, catalogo tributario e anexos
requests | HTTP simples | Primeira tentativa de consulta ao portal de conformidade
selenium | Automacao Web | Fallback do portal de conformidade
lxml | Parser HTML | Extracao dos cards do portal
sqlite3 | Persistencia local | Cache operacional e base de consulta
pandas + openpyxl | Exportacao | Planilhas Excel
python-dotenv | Configuracao | Carregamento de `.env`
zeep | Dependencia legado/compatibilidade | WSDL configuravel, nao e o fluxo principal do modulo C

4. Configuracao e Variaveis de Ambiente
Configuracao carregada em `app/config.py`.

Variaveis suportadas:
- `DB_USER`
- `DB_PASS`
- `DB_DSN`
- `CERT_CAMINHO`
- `CERT_SENHA`
- `ORACLE_CLIENT_CAMINHO`
- `PORTAL_CONFORMIDADE_URL`
- `CFF_WSDL_URL`
- `CFF_API_URL`
- `CFF_ANEXOS_API_URL`
- `CFF_RESPOSTA_EXEMPLO_PATH`
- `CFF_ANEXOS_RESPOSTA_EXEMPLO_PATH`

Valores padrao relevantes:
- SEFAZ GTIN: `https://dfe-servico.svrs.rs.gov.br/ws/ccgConsGTIN/ccgConsGTIN.asmx`
- Portal Conformidade: `https://dfe-portal.svrs.rs.gov.br/CFF/ClassificacaoTributariaNCM`
- Catalogo tributario: `https://cff.svrs.rs.gov.br/api/v1/consultas/classTrib`
- Anexos: `https://cff.svrs.rs.gov.br/api/v1/consultas/anexos`
- SQLite local: `data/consulta_gtin.db`

5. Modulos Funcionais e Integracoes
5.1 Modulo A - Consulta GTIN na SEFAZ
Arquivo: `app/services/sefaz_service.py`

Entrada:
- GTIN validado
- Certificado A1 (`.pfx/.p12`)

Metodo atual:
- POST SOAP via `requests_pkcs12.post`
- Envelope XML gerado manualmente
- Parse do XML em `app/parsers/sefaz_xml.py`

Saida normalizada:
- `status`
- `motivo`
- `xProd`
- `NCM`
- `CEST`
- `data_hora`

Regras operacionais:
- GTIN passa por validacao de DV e prefixo GS1 BR antes da chamada
- Retorno `656` da SEFAZ gera retry simples com pausa

5.2 Modulo B - Cenarios Tributarios por NCM
Arquivo: `app/services/conformidade_scraper_service.py`

Entrada:
- NCM numerico

Metodo atual:
- Tentativa HTTP simples com `requests.get`
- Fallback para Selenium Headless quando necessario
- Parser HTML via `lxml.html`

Extracao atual:
- `cst`
- `cclasstrib`
- `condicao_legal`
- `fonte`

Observacoes:
- O parser reconhece `data-cst`, `data-class-trib`, `card-code` e `onclick`
- O modelo atual persiste cenarios por `NCM`, nao por `GTIN`

5.3 Modulo C - Catalogo e Dossie Tributario
Arquivo: `app/services/dossie_tributario_service.py`

Entrada:
- Certificado A1
- Endpoint autenticado do catalogo tributario

Metodo atual:
- GET via `requests_pkcs12.get`
- Normalizacao do catalogo por `CST`
- Persistencia das classificacoes por `cClassTrib`

Campos mapeados do dossie:
- `cst`
- `cclasstrib`
- `descricao`
- `p_red_ibs`
- `p_red_cbs`
- `tipo_aliquota`
- `publicacao`
- `inicio_vigencia`
- `fim_vigencia`
- `ind_nfe`
- `ind_nfce`
- indicadores adicionais de documentos
- `anexo`
- `base_legal`
- `links_legais`
- `raw_json`

Observacao:
- `CFF_WSDL_URL` permanece configuravel por compatibilidade, mas o fluxo atual do modulo C usa o endpoint REST autenticado.

5.4 Modulo D - Servico de Anexos
Arquivo: `app/services/anexo_tributario_service.py`

Entrada:
- Certificado A1
- Endpoint de anexos

Metodo atual:
- GET via `requests_pkcs12.get`
- Normalizacao de retorno hierarquico ou achatado
- Agrupamento por `anexo`

Campos persistidos:
- Anexo: `anexo`, `descricao`, `publicacao`, `inicio_vigencia`, `fim_vigencia`, `raw_json`
- Especificidade: `codigo`, `descricao`, `valor`, `tipo`, `publicacao`, `inicio_vigencia`, `fim_vigencia`, `raw_json`

6. Orquestracao do Fluxo
Arquivo: `app/services/classificacao_tributaria_service.py`

Fluxo por GTIN:
1. Recebe produto do ERP (`codprod`, `gtin`, `ncm_erp`)
2. Valida GTIN
3. Consulta SEFAZ
4. Persiste consulta GTIN em SQLite
5. Define o NCM para classificacao
6. Consulta cenarios do portal por NCM
7. Busca `cClassTrib` no cache local
8. Se necessario, sincroniza catalogo tributario
9. Se necessario, sincroniza anexos
10. Enriquece cenarios com dossie e persiste resultado

Fluxo direto por NCM:
1. Coleta NCMs do ERP
2. Remove duplicidades
3. Consulta o portal por NCM
4. Enriquece com dossie e anexos
5. Persiste cenarios no SQLite

7. Persistencia SQLite
Arquivo: `app/services/sqlite_service.py`

Tabelas principais:
- `consultas_gtin`
- `catalogo_cst_tributario`
- `dossie_classtrib`
- `cenarios_tributarios`
- `anexos_tributarios`
- `anexos_especificidades`

7.1 `consultas_gtin`
Chave principal:
- `gtin`

Campos principais:
- `cod_winthor`
- `status_sefaz`
- `motivo_sefaz`
- `ncm_winthor`
- `ncm_oficial`
- `divergencia_ncm`
- `descricao_produto`
- `cest`
- `ultima_atualizacao`
- `ultima_atualizacao_ordem`

7.2 `catalogo_cst_tributario`
Chave principal:
- `cst`

Uso:
- cache do catalogo retornado pelo servico tributario

7.3 `dossie_classtrib`
Chave principal:
- `cclasstrib`

Uso:
- cache detalhado por classificacao tributaria

7.4 `cenarios_tributarios`
Chave logica:
- `UNIQUE(ncm, cclasstrib, cst, condicao_legal)`

Uso:
- tabela principal de cenarios por NCM

Campos principais:
- `ncm`
- `cst`
- `cclasstrib`
- `condicao_legal`
- `descricao_dossie`
- `p_red_ibs`
- `p_red_cbs`
- `publicacao`
- `inicio_vigencia`
- `anexo`
- `ind_nfe`
- `ind_nfce`
- `base_legal`
- `fonte`
- `ultima_atualizacao`

7.5 `anexos_tributarios`
Chave principal:
- `anexo`

Uso:
- cache mestre de anexos do servico

7.6 `anexos_especificidades`
Chave:
- `id`

Restricao de unicidade:
- `UNIQUE(anexo, codigo, descricao, valor)`

Uso:
- detalhamento por item ou especificidade de anexo

8. Migracoes e Regras de Integridade
- O repositorio garante criacao automatica das tabelas na inicializacao.
- O schema de `consultas_gtin` e migrado com coluna de ordenacao tecnica.
- O schema legado de `cenarios_tributarios` e migrado para o modelo por NCM.
- O schema de `dossie_classtrib` recebe colunas novas de forma aditiva.
- A sincronizacao completa de anexos substitui o cache anterior para evitar residuos de execucoes antigas.
- Cenarios duplicados do portal sao deduplicados antes da persistencia.

9. Interface Atual
Arquivo principal: `app/gui/interface.py`

Janelas disponiveis:
- Janela principal
- Janela de selecao manual de GTINs
- Janela da base SQLite de consultas GTIN
- Janela da base SQLite de cenarios tributarios
- Janela da base SQLite de anexos e especificidades

Acoes principais da GUI:
- `Atualizar Base SQLite`
- `Selecionar GTINs`
- `Limpar Selecao Manual`
- `Visualizar Base`
- `Visualizar Cenarios`
- `Visualizar Anexos`
- `Exportar Tudo`
- `Consultar NCMs do ERP`
- `Atualizar do Servico`

10. Concorrencia e Responsividade
- A GUI usa `Thread` para tarefas longas.
- A comunicacao de retorno para a UI e feita por `Queue`.
- O estado `executando_validacao` bloqueia execucoes concorrentes.
- Atualizacao de widgets acontece no thread principal via fila.

11. Exportacao
Arquivo: `app/services/relatorio_service.py`

Exportacoes suportadas:
- Consultas GTIN filtradas
- Cenarios tributarios filtrados
- Retorno de anexos filtrado
- Exportacao completa da base de consultas

Formato:
- Excel (`.xlsx`)

12. Tratamento de Erros e Regras Operacionais
- Quantidade de processamento validada entre `1` e `5000`
- GTIN invalido ou fora do prefixo GS1 BR nao segue para a SEFAZ
- Falhas do modulo B geram warning e mantem o fluxo vivo
- Falhas do modulo C geram warning e mantem o cenario base salvo
- Falhas do servico de anexos geram warning ou erro controlado
- Parser da SEFAZ devolve fallback para respostas inesperadas

13. Testes Automatizados Atuais
Diretorio: `tests/`

Cobertura atual inclui:
- normalizacao do modulo B
- normalizacao do modulo C
- normalizacao do servico de anexos
- persistencia e migracoes no SQLite
- deduplicacao de cenarios
- validacao de quantidade
- fluxo de orquestracao por GTIN e por NCM

14. Divida Tecnica Conhecida
- `sefaz_service.py` ainda usa `verify=False`
- `anexo_tributario_service.py` e `dossie_tributario_service.py` ainda usam `verify=False`
- `oracle_service.py` ainda precisa endurecimento de logs e gerenciamento de recursos
- Fallback Selenium ainda abre browser novo por consulta
- O empacotamento final exige tratamento explicito de arquivos auxiliares
