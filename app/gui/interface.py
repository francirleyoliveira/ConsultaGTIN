from __future__ import annotations

import os
from datetime import datetime
from queue import Empty, Queue
from threading import Thread
from tkinter import ttk

import customtkinter as ctk

from app.config import SQLITE_DB_PATH, load_settings
from app.services.ai_classificacao_service import AIClassificationService
from app.services.classificacao_tributaria_service import ClassificacaoTributariaService
from app.services.oracle_service import buscar_gtins_winthor
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository
from app.utils.input_utils import parse_positive_int
from app.validators.gtin import validar_digito_gtin, validar_prefixo_gs1_brasil


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


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
    ("descricao_especificidade", "Descricao Especificidade", 260),
    ("valor", "Valor", 110),
    ("tipo", "Tipo", 110),
    ("especificidade_publicacao", "Pub. Especificidade", 150),
    ("especificidade_inicio_vigencia", "Inicio Vig. Espec.", 150),
    ("ultima_atualizacao", "Atualizado Em", 140),
]


class AnaliseIAWindow(ctk.CTkToplevel):
    def __init__(self, master, ai_service: AIClassificationService):
        super().__init__(master)
        self.ai_service = ai_service
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self.analise_atual: dict | None = None
        self.tipo_contexto_atual = ""
        self.chave_contexto_atual = ""

        self.title("Analise IA de Classificacao")
        self.geometry("1260x780")
        self.minsize(1020, 620)

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        self.lbl_titulo = ctk.CTkLabel(
            self,
            text="Analise assistida por IA",
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        self.lbl_titulo.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.info_frame = ctk.CTkFrame(self)
        self.info_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        self.info_frame.grid_columnconfigure(0, weight=1)
        self.info_frame.grid_columnconfigure(1, weight=1)

        self.lbl_contexto = ctk.CTkLabel(self.info_frame, text="Contexto: aguardando solicitacao", font=ctk.CTkFont(size=12))
        self.lbl_contexto.grid(row=0, column=0, padx=10, pady=8, sticky="w")

        self.lbl_status = ctk.CTkLabel(self.info_frame, text="Status: pronto", font=ctk.CTkFont(size=12))
        self.lbl_status.grid(row=0, column=1, padx=10, pady=8, sticky="e")

        self.textbox = ctk.CTkTextbox(self, font=ctk.CTkFont(family="Consolas", size=12))
        self.textbox.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="nsew")

        self.feedback_frame = ctk.CTkFrame(self)
        self.feedback_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="ew")
        self.feedback_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(self.feedback_frame, text="Observacao do analista:").grid(row=0, column=0, padx=(10, 6), pady=10, sticky="w")
        self.entry_observacao = ctk.CTkEntry(self.feedback_frame)
        self.entry_observacao.grid(row=0, column=1, padx=6, pady=10, sticky="ew")

        self.btn_confirmar = ctk.CTkButton(
            self.feedback_frame,
            text="Confirmar IA",
            command=lambda: self.registrar_feedback("CONFIRMADO"),
            fg_color="#27ae60",
            hover_color="#1e8449",
        )
        self.btn_confirmar.grid(row=0, column=2, padx=6, pady=10)

        self.btn_rejeitar = ctk.CTkButton(
            self.feedback_frame,
            text="Rejeitar IA",
            command=lambda: self.registrar_feedback("REJEITADO"),
            fg_color="#c0392b",
            hover_color="#922b21",
        )
        self.btn_rejeitar.grid(row=0, column=3, padx=6, pady=10)

        self.btn_revisar = ctk.CTkButton(
            self.feedback_frame,
            text="Marcar Revisao",
            command=lambda: self.registrar_feedback("REVISAR_MANUALMENTE"),
            fg_color="#f39c12",
            hover_color="#d68910",
        )
        self.btn_revisar.grid(row=0, column=4, padx=6, pady=10)

        self.lbl_feedback = ctk.CTkLabel(self.feedback_frame, text="")
        self.lbl_feedback.grid(row=1, column=0, columnspan=5, padx=10, pady=(0, 10), sticky="w")

        self._configurar_feedback(False)
        self.after(100, self.processar_fila_ui)

    def _configurar_feedback(self, habilitado: bool) -> None:
        estado = "normal" if habilitado else "disabled"
        for botao in [self.btn_confirmar, self.btn_rejeitar, self.btn_revisar]:
            botao.configure(state=estado)

    def carregar_analise(self, tipo_contexto: str, chave_contexto: str) -> None:
        self.tipo_contexto_atual = str(tipo_contexto or "").upper()
        self.chave_contexto_atual = str(chave_contexto or "").strip()
        self.analise_atual = None
        self.textbox.delete("1.0", "end")
        self.entry_observacao.delete(0, "end")
        self.lbl_feedback.configure(text="")
        self.lbl_contexto.configure(text=f"Contexto: {self.tipo_contexto_atual} {self.chave_contexto_atual}")
        self.lbl_status.configure(text="Status: analisando...")
        self._configurar_feedback(False)
        Thread(target=self._worker_carregar_analise, daemon=True).start()

    def enqueue(self, action: str, payload: object) -> None:
        self.ui_queue.put((action, payload))

    def processar_fila_ui(self) -> None:
        try:
            while True:
                action, payload = self.ui_queue.get_nowait()
                if action == "resultado":
                    self.analise_atual = dict(payload)
                    self._renderizar_analise()
                    self._configurar_feedback(True)
                elif action == "erro":
                    self.lbl_status.configure(text=f"Status: erro - {payload}")
                    self.textbox.delete("1.0", "end")
                    self.textbox.insert("end", f"Falha ao executar a analise: {payload}\n")
                    self._configurar_feedback(False)
                elif action == "feedback":
                    self.lbl_feedback.configure(text=str(payload))
        except Empty:
            pass
        self.after(100, self.processar_fila_ui)

    def _worker_carregar_analise(self) -> None:
        try:
            if self.tipo_contexto_atual == "GTIN":
                resultado = self.ai_service.analisar_gtin(self.chave_contexto_atual)
            else:
                resultado = self.ai_service.analisar_ncm(self.chave_contexto_atual)
            self.enqueue("resultado", resultado)
        except Exception as exc:
            self.enqueue("erro", str(exc))

    def _renderizar_analise(self) -> None:
        if not self.analise_atual:
            return
        contexto = self.analise_atual.get("contexto", {})
        analise = self.analise_atual.get("analise", {})
        payload = analise.get("resultado_json", {}) if isinstance(analise.get("resultado_json"), dict) else {}
        produto = contexto.get("produto", {})
        recomendacao = payload.get("cenario_recomendado") or {}
        alternativas = payload.get("alternativas") or []
        anexos = payload.get("anexos_considerados") or []
        inconsistencias = payload.get("inconsistencias") or []
        dados_faltantes = payload.get("dados_faltantes") or []

        linhas: list[str] = []
        linhas.append("CONTEXTO")
        linhas.append(f"- Tipo: {contexto.get('tipo_contexto', '')}")
        linhas.append(f"- Chave: {contexto.get('chave_contexto', '')}")
        linhas.append(f"- Cod. ERP: {produto.get('cod_winthor', '')}")
        linhas.append(f"- GTIN: {produto.get('gtin', '')}")
        linhas.append(f"- NCM ERP: {produto.get('ncm_erp', '')}")
        linhas.append(f"- NCM GS1: {produto.get('ncm_gs1', '')}")
        linhas.append(f"- Descricao ERP: {produto.get('descricao_erp', '')}")
        linhas.append(f"- Descricao GS1: {produto.get('descricao_gs1', '')}")
        linhas.append(f"- Divergencia: {produto.get('divergencia_ncm', '')}")
        linhas.append("")
        linhas.append("RESUMO")
        linhas.append(f"- {payload.get('resumo', '')}")
        linhas.append("")
        linhas.append("RECOMENDACAO")
        if recomendacao:
            linhas.append(f"- CST sugerido: {recomendacao.get('cst', '')}")
            linhas.append(f"- cClassTrib sugerido: {recomendacao.get('cclasstrib', '')}")
            linhas.append(f"- Score: {recomendacao.get('score', 0)}")
            linhas.append(f"- Anexo: {recomendacao.get('anexo', '')}")
            for motivo in recomendacao.get('motivos_favoraveis', [])[:4]:
                linhas.append(f"- Motivo: {motivo}")
            for restricao in recomendacao.get('restricoes_do_anexo', [])[:4]:
                linhas.append(f"- Restricao do anexo: {restricao}")
            for especificidade in recomendacao.get('especificidades_relevantes', [])[:5]:
                linhas.append(
                    f"- Particularidade relevante: {especificidade.get('codigo', '')} | {especificidade.get('descricao', '')} | {especificidade.get('valor', '')}"
                )
        else:
            linhas.append("- Nenhum cenario recomendado.")

        linhas.append("")
        linhas.append("ALTERNATIVAS")
        if alternativas:
            for alternativa in alternativas[:5]:
                linhas.append(
                    f"- CST {alternativa.get('cst', '')} | cClassTrib {alternativa.get('cclasstrib', '')} | Score {alternativa.get('score', 0)} | Anexo {alternativa.get('anexo', '')}"
                )
        else:
            linhas.append("- Nenhuma alternativa calculada.")

        linhas.append("")
        linhas.append("ANEXOS CONSIDERADOS")
        if anexos:
            for anexo in anexos[:5]:
                linhas.append(
                    f"- Anexo {anexo.get('anexo', '')}: {anexo.get('descricao', '')} | Particularidades: {anexo.get('quantidade_particularidades', 0)}"
                )
                for particularidade in anexo.get('particularidades', [])[:4]:
                    linhas.append(
                        f"  * {particularidade.get('codigo', '')} | {particularidade.get('descricao', '')} | {particularidade.get('valor', '')}"
                    )
        else:
            linhas.append("- Nenhum anexo relacionado encontrado no cache local.")

        linhas.append("")
        linhas.append("INCONSISTENCIAS")
        if inconsistencias:
            for item in inconsistencias:
                linhas.append(f"- {item}")
        else:
            linhas.append("- Nenhuma inconsistencia destacada.")

        linhas.append("")
        linhas.append("DADOS FALTANTES")
        if dados_faltantes:
            for item in dados_faltantes:
                linhas.append(f"- {item}")
        else:
            linhas.append("- Sem lacunas relevantes identificadas pela heuristica atual.")

        self.textbox.delete("1.0", "end")
        self.textbox.insert("end", "\n".join(linhas))
        self.lbl_status.configure(
            text=(
                f"Status: analise concluida | Score {analise.get('score_confianca', 0)} | Revisao humana: {analise.get('requer_revisao_humana', 'S')}"
            )
        )

    def registrar_feedback(self, decisao: str) -> None:
        if not self.analise_atual or not self.analise_atual.get("analise"):
            self.lbl_feedback.configure(text="Nenhuma analise carregada para registrar feedback.")
            return
        analise = self.analise_atual["analise"]
        recomendacao = analise.get("resultado_json", {}).get("cenario_recomendado", {}) if isinstance(analise.get("resultado_json"), dict) else {}
        try:
            feedback_id = self.ai_service.registrar_feedback(
                analise_id=int(analise.get("id", 0)),
                decisao=decisao,
                cst_final=str(recomendacao.get("cst", "") or ""),
                cclasstrib_final=str(recomendacao.get("cclasstrib", "") or ""),
                observacao=self.entry_observacao.get().strip(),
            )
            self.lbl_feedback.configure(text=f"Feedback registrado com sucesso. ID {feedback_id} | Decisao: {decisao}")
        except Exception as exc:
            self.lbl_feedback.configure(text=f"Falha ao registrar feedback: {exc}")



class AsyncTableWindowMixin:
    render_batch_size = 300

    def _init_async_table_loader(self) -> None:
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self._table_request_id = 0
        self._table_render_request_id = 0
        self._table_render_items: list[dict] = []
        self._table_render_index = 0
        self.after(100, self.processar_fila_ui)

    def _solicitar_carga_tabela(self) -> None:
        self._table_request_id += 1
        request_id = self._table_request_id
        filtros = self._obter_filtros()
        self._configurar_estado_tabela(True)
        self.lbl_status.configure(text="Carregando dados...")
        Thread(target=self._worker_carregar_tabela, args=(request_id, filtros), daemon=True).start()

    def _worker_carregar_tabela(self, request_id: int, filtros: dict[str, str]) -> None:
        try:
            dados = self._buscar_dados_tabela(filtros)
            self.ui_queue.put(("dados_tabela", {"request_id": request_id, "dados": dados}))
        except Exception as exc:
            self.ui_queue.put(("erro_tabela", {"request_id": request_id, "erro": str(exc)}))

    def processar_fila_ui(self) -> None:
        try:
            while True:
                action, payload = self.ui_queue.get_nowait()
                if action == "dados_tabela":
                    dados = dict(payload)
                    if int(dados.get("request_id", 0)) != self._table_request_id:
                        continue
                    self.dados_atuais = list(dados.get("dados", []))
                    self.tree.delete(*self.tree.get_children())
                    self._table_render_request_id = self._table_request_id
                    self._table_render_items = list(self.dados_atuais)
                    self._table_render_index = 0
                    self._renderizar_lote_tabela()
                elif action == "erro_tabela":
                    dados = dict(payload)
                    if int(dados.get("request_id", 0)) != self._table_request_id:
                        continue
                    self._configurar_estado_tabela(False)
                    self.lbl_status.configure(text=f"Erro ao carregar dados: {dados.get('erro', '')}")
        except Empty:
            pass
        self.after(100, self.processar_fila_ui)

    def _renderizar_lote_tabela(self) -> None:
        if self._table_render_request_id != self._table_request_id:
            return
        total = len(self._table_render_items)
        if total == 0:
            self._configurar_estado_tabela(False)
            self.lbl_status.configure(text=self._texto_status_concluido(0))
            return

        fim = min(self._table_render_index + self.render_batch_size, total)
        for item in self._table_render_items[self._table_render_index:fim]:
            self._inserir_linha_tabela(item)
        self._table_render_index = fim

        if self._table_render_index < total:
            self.lbl_status.configure(text=f"Carregando {self._table_render_index}/{total}...")
            self.after(1, self._renderizar_lote_tabela)
            return

        self._configurar_estado_tabela(False)
        self.lbl_status.configure(text=self._texto_status_concluido(total))

    def _buscar_dados_tabela(self, filtros: dict[str, str]) -> list[dict]:
        raise NotImplementedError

    def _inserir_linha_tabela(self, item: dict) -> None:
        raise NotImplementedError

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} registro(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        return None


class ConsultaDatabaseWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Consultas GTIN")
        self.geometry("1380x720")
        self.minsize(1100, 600)
        self.dados_atuais: list[dict] = []
        self._init_async_table_loader()

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(
            self,
            text="Consultas persistidas em SQLite",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.filtro_frame = ctk.CTkFrame(self)
        self.filtro_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        for coluna in range(6):
            self.filtro_frame.grid_columnconfigure(coluna, weight=1)

        self.entry_cod_erp = self._criar_filtro("Cod. ERP", 0)
        self.entry_gtin = self._criar_filtro("GTIN", 1)
        self.entry_status = self._criar_filtro("Status", 2)
        self.entry_divergencia = self._criar_filtro("Divergencia", 3)
        self.entry_ncm = self._criar_filtro("NCM", 4)
        self.entry_descricao = self._criar_filtro("Descricao", 5)

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")

        ctk.CTkButton(self.actions_frame, text="Aplicar Filtros", command=self.atualizar_tabela).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Limpar", command=self.limpar_filtros).pack(side="left", padx=(0, 8))
        ctk.CTkButton(
            self.actions_frame,
            text="Exportar Resultado",
            command=self.exportar_resultado,
            fg_color="#2ecc71",
            hover_color="#27ae60",
        ).pack(side="left", padx=(0, 8))
        self.btn_revalidar = ctk.CTkButton(
            self.actions_frame,
            text="Revalidar Selecionados",
            command=self.revalidar_selecionados,
            fg_color="#3498db",
            hover_color="#2980b9",
        )
        self.btn_revalidar.pack(side="left", padx=(0, 8))
        self.btn_analise_ia = ctk.CTkButton(
            self.actions_frame,
            text="Analisar com IA",
            command=self.analisar_selecionado_com_ia,
            fg_color="#8e44ad",
            hover_color="#7d3c98",
        )
        self.btn_analise_ia.pack(side="left")
        self.lbl_status = ctk.CTkLabel(self.actions_frame, text="")
        self.lbl_status.pack(side="right")

        self.table_frame = ctk.CTkFrame(self)
        self.table_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.table_frame.grid_columnconfigure(0, weight=1)
        self.table_frame.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(self.table_frame, columns=[c[0] for c in COLUNAS_TABELA], show="headings")
        for chave, titulo, largura in COLUNAS_TABELA:
            self.tree.heading(chave, text=titulo)
            self.tree.column(chave, width=largura, anchor="w", stretch=True)

        self.tree.tag_configure("ok", foreground="#2ecc71")
        self.tree.tag_configure("divergente", foreground="#e74c3c")
        self.tree.tag_configure("atencao", foreground="#f1c40f")
        self.tree.tag_configure("invalido", foreground="#95a5a6")

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self.after(100, self.atualizar_tabela)

    def _criar_filtro(self, titulo: str, coluna: int) -> ctk.CTkEntry:
        frame = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame.grid(row=0, column=coluna, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame, text=titulo).pack(anchor="w")
        entry = ctk.CTkEntry(frame)
        entry.pack(fill="x", pady=(4, 0))
        entry.bind("<KeyRelease>", lambda _event: self._disparar_busca_programada())
        return entry

    def _disparar_busca_programada(self) -> None:
        if hasattr(self, "_timer_busca"):
            self.after_cancel(self._timer_busca)
        self._timer_busca = self.after(300, self.atualizar_tabela)

    def _obter_filtros(self) -> dict[str, str]:
        return {
            "cod_winthor": self.entry_cod_erp.get(),
            "gtin": self.entry_gtin.get(),
            "status_sefaz": self.entry_status.get(),
            "divergencia_ncm": self.entry_divergencia.get(),
            "ncm": self.entry_ncm.get(),
            "descricao_produto": self.entry_descricao.get(),
        }

    def atualizar_tabela(self) -> None:
        self._solicitar_carga_tabela()

    def _buscar_dados_tabela(self, filtros: dict[str, str]) -> list[dict]:
        return self.repository.listar_consultas(filtros)

    def _inserir_linha_tabela(self, item: dict) -> None:
        valores = [item.get(chave, "") for chave, _, _ in COLUNAS_TABELA]
        divergencia = str(item.get("divergencia_ncm", "")).upper()
        status_sefaz = str(item.get("status_sefaz", ""))
        tag = ""
        if "DIVERGENTE" in divergencia:
            tag = "divergente"
        elif "NAO INFORMADO" in divergencia or "CONSULTA FALHOU" in divergencia:
            tag = "atencao"
        elif "INVALIDO" in divergencia or status_sefaz in {"GTIN_INVALIDO", "GTIN_FORA_GS1_BR"}:
            tag = "invalido"
        elif divergencia in {"OK", "NAO"}:
            tag = "ok"
        self.tree.insert("", "end", values=valores, tags=(tag,))

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} registro(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        estado = "disabled" if carregando or self.master.executando_validacao else "normal"
        self.btn_revalidar.configure(state=estado)
        self.btn_analise_ia.configure(state=estado)

    def limpar_filtros(self) -> None:
        for entry in [self.entry_cod_erp, self.entry_gtin, self.entry_status, self.entry_divergencia, self.entry_ncm, self.entry_descricao]:
            entry.delete(0, "end")
        self.atualizar_tabela()

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum registro para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_SQLite_GTIN")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def revalidar_selecionados(self) -> None:
        if self.master.executando_validacao:
            self.lbl_status.configure(text="Aguarde a validacao atual terminar")
            return
        selecionados_ids = self.tree.selection()
        if not selecionados_ids:
            self.lbl_status.configure(text="Selecione itens na tabela primeiro")
            return
        produtos_para_revalidar = []
        indices = {chave: idx for idx, (chave, _, _) in enumerate(COLUNAS_TABELA)}
        for item_id in selecionados_ids:
            valores = self.tree.item(item_id, "values")
            produtos_para_revalidar.append((
                valores[indices["cod_winthor"]],
                valores[indices["gtin"]],
                valores[indices["ncm_winthor"]],
                valores[indices["descricao_erp"]],
            ))
        self.master.produtos_selecionados = produtos_para_revalidar
        self.master.atualizar_resumo_selecao()
        self.master.start_validation_thread()

    def analisar_selecionado_com_ia(self) -> None:
        selecionados_ids = self.tree.selection()
        if len(selecionados_ids) != 1:
            self.lbl_status.configure(text="Selecione exatamente um GTIN para analisar com IA")
            return
        indices = {chave: idx for idx, (chave, _, _) in enumerate(COLUNAS_TABELA)}
        valores = self.tree.item(selecionados_ids[0], "values")
        gtin = str(valores[indices["gtin"]] or "").strip()
        if not gtin:
            self.lbl_status.configure(text="GTIN invalido para analise")
            return
        self.master.abrir_analise_ia("GTIN", gtin)


class CenariosTributariosWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Cenarios Tributarios por NCM")
        self.geometry("1460x760")
        self.minsize(1180, 620)
        self.dados_atuais: list[dict] = []
        self._init_async_table_loader()

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(
            self,
            text="Cenarios tributarios persistidos por NCM",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.filtro_frame = ctk.CTkFrame(self)
        self.filtro_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        for coluna in range(5):
            self.filtro_frame.grid_columnconfigure(coluna, weight=1)

        self.entry_ncm = self._criar_filtro("NCM", 0)
        self.entry_cst = self._criar_filtro("CST", 1)
        self.entry_cclasstrib = self._criar_filtro("cClassTrib", 2)
        self.entry_condicao = self._criar_filtro("Condicao", 3)
        self.entry_descricao = self._criar_filtro("Descricao", 4)

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")
        ctk.CTkButton(self.actions_frame, text="Aplicar Filtros", command=self.atualizar_tabela).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Limpar", command=self.limpar_filtros).pack(side="left", padx=(0, 8))
        ctk.CTkButton(
            self.actions_frame,
            text="Exportar Resultado",
            command=self.exportar_resultado,
            fg_color="#2ecc71",
            hover_color="#27ae60",
        ).pack(side="left", padx=(0, 8))
        self.btn_consultar_erp = ctk.CTkButton(
            self.actions_frame,
            text="Consultar NCMs do ERP",
            command=self.consultar_ncms_erp,
            fg_color="#e67e22",
            hover_color="#ca6f1e",
        )
        self.btn_consultar_erp.pack(side="left", padx=(0, 8))
        self.btn_analise_ia = ctk.CTkButton(
            self.actions_frame,
            text="Analisar NCM com IA",
            command=self.analisar_ncm_com_ia,
            fg_color="#8e44ad",
            hover_color="#7d3c98",
        )
        self.btn_analise_ia.pack(side="left")
        self.lbl_status = ctk.CTkLabel(self.actions_frame, text="")
        self.lbl_status.pack(side="right")

        self.table_frame = ctk.CTkFrame(self)
        self.table_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.table_frame.grid_columnconfigure(0, weight=1)
        self.table_frame.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(self.table_frame, columns=[c[0] for c in COLUNAS_CENARIOS], show="headings")
        for chave, titulo, largura in COLUNAS_CENARIOS:
            self.tree.heading(chave, text=titulo)
            self.tree.column(chave, width=largura, anchor="w", stretch=True)

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self.after(100, self.atualizar_tabela)

    def _criar_filtro(self, titulo: str, coluna: int) -> ctk.CTkEntry:
        frame = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame.grid(row=0, column=coluna, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame, text=titulo).pack(anchor="w")
        entry = ctk.CTkEntry(frame)
        entry.pack(fill="x", pady=(4, 0))
        entry.bind("<KeyRelease>", lambda _event: self._disparar_busca_programada())
        return entry

    def _disparar_busca_programada(self) -> None:
        if hasattr(self, "_timer_busca"):
            self.after_cancel(self._timer_busca)
        self._timer_busca = self.after(300, self.atualizar_tabela)

    def _obter_filtros(self) -> dict[str, str]:
        return {
            "ncm": self.entry_ncm.get(),
            "cst": self.entry_cst.get(),
            "cclasstrib": self.entry_cclasstrib.get(),
            "condicao_legal": self.entry_condicao.get(),
            "descricao_dossie": self.entry_descricao.get(),
        }

    def atualizar_tabela(self) -> None:
        self._solicitar_carga_tabela()

    def _buscar_dados_tabela(self, filtros: dict[str, str]) -> list[dict]:
        return self.repository.listar_cenarios_tributarios(filtros)

    def _inserir_linha_tabela(self, item: dict) -> None:
        self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in COLUNAS_CENARIOS])

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} cenario(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        estado = "disabled" if carregando or self.master.executando_validacao else "normal"
        self.btn_consultar_erp.configure(state=estado)
        self.btn_analise_ia.configure(state=estado)

    def limpar_filtros(self) -> None:
        for entry in [self.entry_ncm, self.entry_cst, self.entry_cclasstrib, self.entry_condicao, self.entry_descricao]:
            entry.delete(0, "end")
        self.atualizar_tabela()

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum cenario para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_Cenarios_Tributarios")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def consultar_ncms_erp(self) -> None:
        filtro_ncm = self.entry_ncm.get().strip()
        self.master.start_ncm_sync_thread(filtro_ncm)

    def analisar_ncm_com_ia(self) -> None:
        selecionados_ids = self.tree.selection()
        if len(selecionados_ids) != 1:
            self.lbl_status.configure(text="Selecione exatamente um cenario para analisar o NCM com IA")
            return
        indices = {chave: idx for idx, (chave, _, _) in enumerate(COLUNAS_CENARIOS)}
        valores = self.tree.item(selecionados_ids[0], "values")
        ncm = str(valores[indices["ncm"]] or "").strip()
        if not ncm:
            self.lbl_status.configure(text="NCM invalido para analise")
            return
        self.master.abrir_analise_ia("NCM", ncm)

class AnexosTributariosWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Retorno dos Anexos")
        self.geometry("1500x760")
        self.minsize(1200, 620)
        self.dados_atuais: list[dict] = []
        self._init_async_table_loader()

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(
            self,
            text="Retorno dos anexos e suas especificidades",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.filtro_frame = ctk.CTkFrame(self)
        self.filtro_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        for coluna in range(5):
            self.filtro_frame.grid_columnconfigure(coluna, weight=1)

        self.entry_anexo = self._criar_filtro("Anexo", 0)
        self.entry_descricao = self._criar_filtro("Descricao Anexo", 1)
        self.entry_codigo = self._criar_filtro("Cod. Especificidade", 2)
        self.entry_desc_esp = self._criar_filtro("Descricao Especificidade", 3)
        self.entry_tipo = self._criar_filtro("Tipo", 4)

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")
        ctk.CTkButton(self.actions_frame, text="Aplicar Filtros", command=self.atualizar_tabela).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Limpar", command=self.limpar_filtros).pack(side="left", padx=(0, 8))
        ctk.CTkButton(
            self.actions_frame,
            text="Exportar Resultado",
            command=self.exportar_resultado,
            fg_color="#2ecc71",
            hover_color="#27ae60",
        ).pack(side="left", padx=(0, 8))
        self.btn_atualizar_servico = ctk.CTkButton(
            self.actions_frame,
            text="Atualizar do Servico",
            command=self.atualizar_do_servico,
            fg_color="#e67e22",
            hover_color="#ca6f1e",
        )
        self.btn_atualizar_servico.pack(side="left", padx=(0, 8))
        self.lbl_status = ctk.CTkLabel(self.actions_frame, text="")
        self.lbl_status.pack(side="right")

        self.table_frame = ctk.CTkFrame(self)
        self.table_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.table_frame.grid_columnconfigure(0, weight=1)
        self.table_frame.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(self.table_frame, columns=[c[0] for c in COLUNAS_ANEXOS], show="headings")
        for chave, titulo, largura in COLUNAS_ANEXOS:
            self.tree.heading(chave, text=titulo)
            self.tree.column(chave, width=largura, anchor="w", stretch=True)

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self.after(100, self.atualizar_tabela)

    def _criar_filtro(self, titulo: str, coluna: int) -> ctk.CTkEntry:
        frame = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame.grid(row=0, column=coluna, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame, text=titulo).pack(anchor="w")
        entry = ctk.CTkEntry(frame)
        entry.pack(fill="x", pady=(4, 0))
        entry.bind("<KeyRelease>", lambda _event: self._disparar_busca_programada())
        return entry

    def _disparar_busca_programada(self) -> None:
        if hasattr(self, "_timer_busca"):
            self.after_cancel(self._timer_busca)
        self._timer_busca = self.after(300, self.atualizar_tabela)

    def _obter_filtros(self) -> dict[str, str]:
        return {
            "anexo": self.entry_anexo.get(),
            "descricao": self.entry_descricao.get(),
            "codigo_especificidade": self.entry_codigo.get(),
            "descricao_especificidade": self.entry_desc_esp.get(),
            "tipo": self.entry_tipo.get(),
        }

    def atualizar_tabela(self) -> None:
        self._solicitar_carga_tabela()

    def _buscar_dados_tabela(self, filtros: dict[str, str]) -> list[dict]:
        return self.repository.listar_retorno_anexos(filtros)

    def _inserir_linha_tabela(self, item: dict) -> None:
        self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in COLUNAS_ANEXOS])

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} linha(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        estado = "disabled" if carregando or self.master.executando_validacao else "normal"
        self.btn_atualizar_servico.configure(state=estado)

    def limpar_filtros(self) -> None:
        for entry in [self.entry_anexo, self.entry_descricao, self.entry_codigo, self.entry_desc_esp, self.entry_tipo]:
            entry.delete(0, "end")
        self.atualizar_tabela()

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum anexo para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_Retorno_Anexos")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def atualizar_do_servico(self) -> None:
        self.master.start_anexo_sync_thread()


class SelecaoGtinsWindow(ctk.CTkToplevel):
    def __init__(self, master, settings, repository: ConsultaGtinRepository, on_confirmar):
        super().__init__(master)
        self.settings = settings
        self.repository = repository
        self.on_confirmar = on_confirmar
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self.todos_produtos: list[tuple] = []
        self.produtos_visiveis: list[tuple] = []
        self.status_por_gtin: dict[str, dict[str, str]] = {}

        self.title("Selecionar GTINs para consulta")
        self.geometry("1480x720")
        self.minsize(1180, 580)

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        self.header = ctk.CTkLabel(
            self,
            text="Escolha quais GTINs devem ser consultados",
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        self.header.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.filtro_frame = ctk.CTkFrame(self)
        self.filtro_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        for coluna in range(5):
            self.filtro_frame.grid_columnconfigure(coluna, weight=1)

        self.entry_cod = self._criar_filtro("Cod. Winthor", 0)
        self.entry_gtin = self._criar_filtro("GTIN", 1)
        self.entry_ncm = self._criar_filtro("NCM", 2)
        self.entry_status_consulta = self._criar_filtro("Status da Consulta", 3)

        frame_limite = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame_limite.grid(row=0, column=4, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame_limite, text="Maximo na grade").pack(anchor="w")
        self.entry_limite = ctk.CTkEntry(frame_limite)
        self.entry_limite.insert(0, "500")
        self.entry_limite.pack(fill="x", pady=(4, 0))
        self.entry_limite.bind("<Return>", lambda _event: self.aplicar_filtros())

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")

        self.btn_carregar = ctk.CTkButton(self.actions_frame, text="Carregar GTINs", command=self.carregar_produtos)
        self.btn_carregar.pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Aplicar Filtros", command=self.aplicar_filtros).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Selecionar Visiveis", command=self.selecionar_visiveis).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Limpar Selecao", command=self.limpar_selecao).pack(side="left", padx=(0, 8))
        ctk.CTkButton(
            self.actions_frame,
            text="Usar Selecao",
            command=self.confirmar_selecao,
            fg_color="#2ecc71",
            hover_color="#27ae60",
        ).pack(side="left")

        self.lbl_status = ctk.CTkLabel(self.actions_frame, text="")
        self.lbl_status.pack(side="right")

        self.table_frame = ctk.CTkFrame(self)
        self.table_frame.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.table_frame.grid_columnconfigure(0, weight=1)
        self.table_frame.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            self.table_frame,
            columns=["codprod", "gtin", "ncm", "descricao_erp", "status_consulta"],
            show="headings",
            selectmode="extended",
        )
        self.tree.heading("codprod", text="Cod. Winthor")
        self.tree.heading("gtin", text="GTIN")
        self.tree.heading("ncm", text="NCM")
        self.tree.heading("descricao_erp", text="Descricao ERP")
        self.tree.heading("status_consulta", text="Status da Consulta")
        self.tree.column("codprod", width=140, anchor="w")
        self.tree.column("gtin", width=180, anchor="w")
        self.tree.column("ncm", width=160, anchor="w")
        self.tree.column("descricao_erp", width=320, anchor="w")
        self.tree.column("status_consulta", width=280, anchor="w")

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self.after(100, self.processar_fila_ui)
        self.lbl_status.configure(text="Clique em Carregar GTINs para iniciar")

    def _criar_filtro(self, titulo: str, coluna: int) -> ctk.CTkEntry:
        frame = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame.grid(row=0, column=coluna, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame, text=titulo).pack(anchor="w")
        entry = ctk.CTkEntry(frame)
        entry.pack(fill="x", pady=(4, 0))
        entry.bind("<Return>", lambda _event: self.aplicar_filtros())
        return entry

    def enqueue(self, action: str, payload: object) -> None:
        self.ui_queue.put((action, payload))

    def processar_fila_ui(self) -> None:
        try:
            while True:
                action, payload = self.ui_queue.get_nowait()
                if action == "status":
                    self.lbl_status.configure(text=str(payload))
                elif action == "dados":
                    dados = dict(payload)
                    self.todos_produtos = list(dados.get("produtos", []))
                    self.status_por_gtin = dict(dados.get("status_por_gtin", {}))
                    self.aplicar_filtros()
                    self.btn_carregar.configure(state="normal")
                elif action == "erro":
                    self.lbl_status.configure(text=str(payload))
                    self.btn_carregar.configure(state="normal")
        except Empty:
            pass
        self.after(100, self.processar_fila_ui)

    def carregar_produtos(self) -> None:
        self.btn_carregar.configure(state="disabled")
        self.lbl_status.configure(text="Carregando produtos do Winthor...")
        Thread(target=self._carregar_produtos_worker, daemon=True).start()

    def _carregar_produtos_worker(self) -> None:
        try:
            produtos = buscar_gtins_winthor(self.settings)
            gtins = [str(produto[1] or "") for produto in produtos]
            status_por_gtin = self.repository.buscar_status_por_gtins(gtins)
            self.enqueue("dados", {"produtos": produtos, "status_por_gtin": status_por_gtin})
            self.enqueue("status", f"{len(produtos)} produto(s) carregado(s)")
        except Exception as erro:
            self.enqueue("erro", f"Erro ao carregar GTINs: {erro}")

    def _montar_status_consulta(self, gtin_valor: str) -> str:
        status = self.status_por_gtin.get(gtin_valor)
        if not status:
            return "Nao consultado"
        if status.get("divergencia_ncm", "").strip():
            return status["divergencia_ncm"].strip()
        if status.get("status_sefaz", "").strip():
            return status["status_sefaz"].strip()
        return "Consultado"

    def _obter_limite(self) -> int:
        try:
            limite = int(self.entry_limite.get().strip() or "500")
            return max(1, min(limite, 5000))
        except ValueError:
            self.entry_limite.delete(0, "end")
            self.entry_limite.insert(0, "500")
            return 500

    def aplicar_filtros(self) -> None:
        cod = self.entry_cod.get().strip()
        gtin = self.entry_gtin.get().strip()
        ncm = self.entry_ncm.get().strip()
        status_consulta_filtro = self.entry_status_consulta.get().strip().lower()
        limite = self._obter_limite()

        self.produtos_visiveis = []
        self.tree.delete(*self.tree.get_children())
        total_filtrado = 0
        for produto in self.todos_produtos:
            codprod, gtin_valor, ncm_valor = produto[0], str(produto[1] or ""), str(produto[2] or "")
            descricao_erp = str(produto[3] or "") if len(produto) > 3 else ""
            if cod and cod not in str(codprod):
                continue
            if gtin and gtin not in gtin_valor:
                continue
            if ncm and ncm not in ncm_valor:
                continue
            status_consulta = self._montar_status_consulta(gtin_valor)
            if status_consulta_filtro and status_consulta_filtro not in status_consulta.lower():
                continue
            total_filtrado += 1
            self.produtos_visiveis.append(produto)
            if len(self.produtos_visiveis) <= limite:
                self.tree.insert("", "end", iid=f"{codprod}|{gtin_valor}", values=(codprod, gtin_valor, ncm_valor, descricao_erp, status_consulta))
        if total_filtrado > limite:
            self.lbl_status.configure(text=f"{total_filtrado} item(ns) encontrado(s), exibindo os primeiros {limite}")
        else:
            self.lbl_status.configure(text=f"{total_filtrado} item(ns) encontrado(s)")
        self.produtos_visiveis = self.produtos_visiveis[:limite]

    def selecionar_visiveis(self) -> None:
        itens = self.tree.get_children()
        if itens:
            self.tree.selection_set(itens)
            self.lbl_status.configure(text=f"{len(itens)} item(ns) selecionado(s)")

    def limpar_selecao(self) -> None:
        self.tree.selection_remove(self.tree.selection())
        self.lbl_status.configure(text="Selecao limpa")

    def confirmar_selecao(self) -> None:
        selecionados = []
        ids = set(self.tree.selection())
        for produto in self.produtos_visiveis:
            codprod, gtin_valor = produto[0], str(produto[1] or "")
            if f"{codprod}|{gtin_valor}" in ids:
                selecionados.append(produto)
        if not selecionados:
            self.lbl_status.configure(text="Selecione ao menos um GTIN")
            return
        self.on_confirmar(selecionados)
        self.destroy()

class AppGTIN(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.settings = load_settings()
        self.repository = ConsultaGtinRepository()
        self.classificacao_service = ClassificacaoTributariaService(self.settings, self.repository)
        self.ai_service = AIClassificationService(self.settings, self.repository)
        self.database_window: ConsultaDatabaseWindow | None = None
        self.cenarios_window: CenariosTributariosWindow | None = None
        self.anexos_window: AnexosTributariosWindow | None = None
        self.selecao_window: SelecaoGtinsWindow | None = None
        self.analise_window: AnaliseIAWindow | None = None
        self.produtos_selecionados: list[tuple] = []
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self.executando_validacao = False

        self.title("Consulta GTIN - Winthor x Sefaz SVRS")
        self.geometry("1040x760")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        self.header = ctk.CTkLabel(
            self,
            text="Validacao e classificacao tributaria por GTIN",
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        self.header.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nw")

        self.config_frame = ctk.CTkFrame(self)
        self.config_frame.grid(row=1, column=0, padx=20, pady=10, sticky="nsew")
        for coluna in range(4):
            self.config_frame.grid_columnconfigure(coluna, weight=1 if coluna == 0 else 0)

        status_cert = "Conectado" if os.path.exists(self.settings.cert_caminho or "") else "Nao Encontrado"
        self.lbl_cert = ctk.CTkLabel(self.config_frame, text=f"Certificado: {status_cert}", font=ctk.CTkFont(size=12))
        self.lbl_cert.grid(row=0, column=0, padx=10, pady=5, sticky="w")

        self.lbl_ws_gtin = ctk.CTkLabel(self.config_frame, text="WS GTIN: aguardando preflight", font=ctk.CTkFont(size=12))
        self.lbl_ws_gtin.grid(row=0, column=3, padx=10, pady=5, sticky="e")

        ctk.CTkLabel(self.config_frame, text="Produtos para validar:").grid(row=0, column=1, padx=(10, 5), pady=5, sticky="e")
        self.entry_qtd = ctk.CTkEntry(self.config_frame, width=80)
        self.entry_qtd.insert(0, "10")
        self.entry_qtd.grid(row=0, column=2, padx=10, pady=5)

        self.lbl_db = ctk.CTkLabel(self.config_frame, text=f"SQLite: {SQLITE_DB_PATH.name}", font=ctk.CTkFont(size=12))
        self.lbl_db.grid(row=1, column=0, padx=10, pady=(0, 5), sticky="w")

        self.lbl_total = ctk.CTkLabel(self.config_frame, text="Registros salvos: 0", font=ctk.CTkFont(size=12))
        self.lbl_total.grid(row=1, column=1, columnspan=2, padx=10, pady=(0, 5), sticky="e")

        self.lbl_selecao = ctk.CTkLabel(self.config_frame, text="Selecao manual: usando quantidade automatica", font=ctk.CTkFont(size=12))
        self.lbl_selecao.grid(row=2, column=0, columnspan=3, padx=10, pady=(0, 5), sticky="w")

        self.textbox = ctk.CTkTextbox(self, font=ctk.CTkFont(family="Consolas", size=12))
        self.textbox.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")
        self.textbox.tag_config("info", foreground="#3498db")
        self.textbox.tag_config("sucesso", foreground="#2ecc71")
        self.textbox.tag_config("erro", foreground="#e74c3c")
        self.textbox.tag_config("alerta", foreground="#f1c40f")

        self.dash_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.dash_frame.grid(row=3, column=0, padx=20, pady=(0, 10), sticky="ew")
        for i in range(5):
            self.dash_frame.grid_columnconfigure(i, weight=1)

        self.card_total = self._criar_card("Total Base", "#34495e", 0)
        self.card_ok = self._criar_card("Status OK", "#27ae60", 1)
        self.card_div = self._criar_card("Divergentes", "#c0392b", 2)
        self.card_outros = self._criar_card("Outros/Alertas", "#f39c12", 3)
        self.card_cenarios = self._criar_card("Cenarios", "#2980b9", 4)

        self.progress = ctk.CTkProgressBar(self)
        self.progress.grid(row=4, column=0, padx=20, pady=5, sticky="ew")
        self.progress.set(0)

        self.button_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.button_frame.grid(row=5, column=0, padx=20, pady=(10, 20), sticky="ew")

        self.btn_iniciar = ctk.CTkButton(self.button_frame, text="Atualizar Base SQLite", command=self.start_validation_thread)
        self.btn_iniciar.pack(side="left", padx=5)
        self.btn_sincronizar_erp = ctk.CTkButton(self.button_frame, text="Sincronizar ERP", command=self.start_erp_sync_thread, fg_color="#16a085", hover_color="#138d75")
        self.btn_sincronizar_erp.pack(side="left", padx=5)
        self.btn_selecionar = ctk.CTkButton(self.button_frame, text="Selecionar GTINs", command=self.abrir_selecao_gtins)
        self.btn_selecionar.pack(side="left", padx=5)
        self.btn_limpar_selecao = ctk.CTkButton(self.button_frame, text="Limpar Selecao Manual", command=self.limpar_selecao_manual)
        self.btn_limpar_selecao.pack(side="left", padx=5)
        self.btn_visualizar = ctk.CTkButton(self.button_frame, text="Visualizar Base", command=self.abrir_visualizador, fg_color="#2ecc71", hover_color="#27ae60")
        self.btn_visualizar.pack(side="left", padx=5)
        self.btn_visualizar_cenarios = ctk.CTkButton(self.button_frame, text="Visualizar Cenarios", command=self.abrir_visualizador_cenarios, fg_color="#3498db", hover_color="#2980b9")
        self.btn_visualizar_cenarios.pack(side="left", padx=5)
        self.btn_visualizar_anexos = ctk.CTkButton(self.button_frame, text="Visualizar Anexos", command=self.abrir_visualizador_anexos, fg_color="#9b59b6", hover_color="#8e44ad")
        self.btn_visualizar_anexos.pack(side="left", padx=5)
        self.btn_exportar = ctk.CTkButton(self.button_frame, text="Exportar Tudo", command=self.exportar_tudo)
        self.btn_exportar.pack(side="left", padx=5)

        self.log("Sistema pronto. As consultas GTIN e os cenarios tributarios por NCM serao persistidos em SQLite.")
        self.atualizar_resumo_base()
        self.after(100, self.processar_fila_ui)

    def enqueue(self, action: str, payload: object) -> None:
        self.ui_queue.put((action, payload))

    def processar_fila_ui(self) -> None:
        try:
            while True:
                action, payload = self.ui_queue.get_nowait()
                if action == "log":
                    if isinstance(payload, tuple):
                        self.log(payload[0], payload[1])
                    else:
                        self.log(str(payload))
                elif action == "progress":
                    self.progress.set(float(payload))
                elif action == "gtin_health":
                    self._atualizar_status_ws_gtin(payload if isinstance(payload, dict) else {})
                elif action in {"finish", "enable_start"}:
                    self.executando_validacao = False
                    self._configurar_botoes_em_execucao(False)
                    self.atualizar_resumo_base()
                    if self.database_window and self.database_window.winfo_exists():
                        self.database_window.atualizar_tabela()
                    if self.cenarios_window and self.cenarios_window.winfo_exists():
                        self.cenarios_window.atualizar_tabela()
                    if self.anexos_window and self.anexos_window.winfo_exists():
                        self.anexos_window.atualizar_tabela()
                elif action == "reset_buttons":
                    self.executando_validacao = True
                    self._configurar_botoes_em_execucao(True)
                    self.progress.set(0)
        except Empty:
            pass
        self.after(100, self.processar_fila_ui)

    def _configurar_botoes_em_execucao(self, em_execucao: bool) -> None:
        estado = "disabled" if em_execucao else "normal"
        for botao in [self.btn_iniciar, self.btn_sincronizar_erp, self.btn_selecionar, self.btn_limpar_selecao, self.btn_visualizar, self.btn_visualizar_cenarios, self.btn_visualizar_anexos, self.btn_exportar]:
            botao.configure(state=estado)

    def log(self, text: str, level: str = "info") -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.textbox.insert("end", f"[{timestamp}] ", level)
        self.textbox.insert("end", f"{text}\n")
        self.textbox.see("end")

    def _atualizar_status_ws_gtin(self, info: dict | None = None) -> None:
        info = info or {}
        texto = "WS GTIN: aguardando preflight"
        cor = "#95a5a6"
        if info.get("ok"):
            texto = "WS GTIN: disponivel"
            if info.get("from_cache"):
                texto += " (cache)"
            cor = "#2ecc71"
        elif info.get("blocked"):
            texto = "WS GTIN: circuito aberto"
            cor = "#e74c3c"
        elif info:
            texto = "WS GTIN: instavel"
            cor = "#f1c40f"
        self.lbl_ws_gtin.configure(text=texto, text_color=cor)

    def _criar_card(self, titulo: str, cor: str, coluna: int) -> ctk.CTkLabel:
        frame = ctk.CTkFrame(self.dash_frame, fg_color=cor, corner_radius=10)
        frame.grid(row=0, column=coluna, padx=5, pady=5, sticky="ew")
        ctk.CTkLabel(frame, text=titulo, font=ctk.CTkFont(size=12)).pack(pady=(10, 0))
        label_valor = ctk.CTkLabel(frame, text="0", font=ctk.CTkFont(size=24, weight="bold"))
        label_valor.pack(pady=(0, 10))
        return label_valor

    def atualizar_resumo_base(self) -> None:
        resumo = self.repository.obter_resumo_estatistico()
        self.card_total.configure(text=str(resumo["total"]))
        self.card_ok.configure(text=str(resumo["ok"]))
        self.card_div.configure(text=str(resumo["divergentes"]))
        self.card_outros.configure(text=str(resumo["outros"]))
        self.card_cenarios.configure(text=str(resumo["total_cenarios"]))
        self.lbl_total.configure(text=f"Registros salvos: {resumo['total']}")

    def atualizar_resumo_selecao(self) -> None:
        if self.produtos_selecionados:
            self.lbl_selecao.configure(text=f"Selecao manual: {len(self.produtos_selecionados)} GTIN(s) escolhido(s)")
        else:
            self.lbl_selecao.configure(text="Selecao manual: usando quantidade automatica")

    def abrir_selecao_gtins(self) -> None:
        if self.selecao_window and self.selecao_window.winfo_exists():
            self.selecao_window.focus()
            return
        self.selecao_window = SelecaoGtinsWindow(self, self.settings, self.repository, self.definir_produtos_selecionados)
        self.selecao_window.focus()

    def definir_produtos_selecionados(self, produtos: list[tuple]) -> None:
        self.produtos_selecionados = produtos
        self.atualizar_resumo_selecao()
        self.log(f"Selecao manual definida com {len(produtos)} GTIN(s).")

    def limpar_selecao_manual(self) -> None:
        self.produtos_selecionados = []
        self.atualizar_resumo_selecao()
        self.log("Selecao manual removida. O fluxo voltou para a quantidade automatica.", "info")

    def _obter_quantidade_limite(self) -> int | None:
        try:
            quantidade = parse_positive_int(self.entry_qtd.get(), default=10, minimum=1, maximum=5000)
        except ValueError:
            self.log("Quantidade invalida. Informe um numero inteiro entre 1 e 5000.", "alerta")
            self.entry_qtd.delete(0, "end")
            self.entry_qtd.insert(0, "10")
            return None
        texto_atual = self.entry_qtd.get().strip()
        if texto_atual != str(quantidade):
            self.entry_qtd.delete(0, "end")
            self.entry_qtd.insert(0, str(quantidade))
        return quantidade

    def start_validation_thread(self) -> None:
        if self.executando_validacao:
            self.log("Ja existe uma validacao em andamento. Aguarde a conclusao.", "alerta")
            return
        qtd_max = None
        if not self.produtos_selecionados:
            qtd_max = self._obter_quantidade_limite()
            if qtd_max is None:
                return
        self.enqueue("reset_buttons", None)
        Thread(target=self.run_validation, args=(qtd_max,), daemon=True).start()

    def start_erp_sync_thread(self) -> None:
        if self.executando_validacao:
            self.log("Ja existe uma validacao em andamento. Aguarde a conclusao.", "alerta")
            return
        self.enqueue("reset_buttons", None)
        Thread(target=self.run_erp_sync, daemon=True).start()

    def start_ncm_sync_thread(self, filtro_ncm: str = "") -> None:
        if self.executando_validacao:
            self.log("Ja existe uma validacao em andamento. Aguarde a conclusao.", "alerta")
            return
        qtd_max = None
        if not "".join(filter(str.isdigit, str(filtro_ncm or ""))):
            qtd_max = self._obter_quantidade_limite()
            if qtd_max is None:
                return
        self.enqueue("reset_buttons", None)
        Thread(target=self.run_ncm_sync, args=(filtro_ncm, qtd_max), daemon=True).start()

    def start_anexo_sync_thread(self) -> None:
        if self.executando_validacao:
            self.log("Ja existe uma validacao em andamento. Aguarde a conclusao.", "alerta")
            return
        self.enqueue("reset_buttons", None)
        Thread(target=self.run_anexo_sync, daemon=True).start()

    def run_erp_sync(self) -> None:
        try:
            self.enqueue("log", "Buscando produtos no Winthor para sincronizar os dados do ERP na base local...")
            produtos_winthor = buscar_gtins_winthor(self.settings)
            if not produtos_winthor:
                self.enqueue("log", ("Nenhum produto encontrado ou erro no banco.", "alerta"))
                self.enqueue("enable_start", None)
                return

            self.enqueue("progress", 0.5)
            resumo = self.repository.sincronizar_consultas_com_erp(produtos_winthor)
            self.enqueue("progress", 1.0)
            self.enqueue(
                "log",
                (
                    f"Sincronizacao ERP concluida: {resumo['atualizados']} consulta(s) local(is) atualizada(s) e {resumo['recalculados']} divergencia(s) recalculada(s), sem nova consulta a SEFAZ.",
                    "sucesso",
                ),
            )
            self.enqueue("finish", None)
        except Exception as erro:
            self.enqueue("log", (f"ERRO CRITICO: {erro}", "erro"))
            self.enqueue("enable_start", None)

    def run_validation(self, qtd_max: int | None = None) -> None:
        try:
            if self.produtos_selecionados:
                produtos = self.produtos_selecionados
                self.enqueue("log", f"Usando selecao manual com {len(produtos)} GTIN(s)...")
            else:
                qtd_max = qtd_max if qtd_max is not None else 10
                self.enqueue("log", "Buscando produtos no Winthor...")
                produtos_winthor = buscar_gtins_winthor(self.settings)
                if not produtos_winthor:
                    self.enqueue("log", ("Nenhum produto encontrado ou erro no banco.", "alerta"))
                    self.enqueue("enable_start", None)
                    return
                produtos = produtos_winthor[: min(len(produtos_winthor), qtd_max)]
                self.enqueue("log", f"Iniciando atualizacao automatica de {len(produtos)} produtos...")

            precisa_preflight = any(
                validar_digito_gtin(str(produto[1] or "")) and validar_prefixo_gs1_brasil(str(produto[1] or ""))
                for produto in produtos
            )
            if precisa_preflight:
                saude_gtin = self.classificacao_service.verificar_saude_servico_gtin(force=False)
                self.enqueue("gtin_health", saude_gtin)
                self.enqueue("log", (str(saude_gtin.get("message") or "Preflight GTIN executado."), "sucesso" if saude_gtin.get("ok") else "alerta"))
                if not saude_gtin.get("ok"):
                    self.enqueue(
                        "log",
                        (
                            "Servico GTIN indisponivel: o lote vai continuar usando cache oficial GS1 quando houver historico salvo no SQLite.",
                            "alerta",
                        ),
                    )

            total = max(len(produtos), 1)
            circuito_alertado = False
            for i, produto in enumerate(produtos):
                self.enqueue("progress", (i + 1) / total)
                resultado = self.classificacao_service.processar_produto(produto)
                consulta = resultado["consulta"]
                gtin = consulta["gtin"]
                codprod = consulta["cod_winthor"]
                status = consulta["status_sefaz"]
                motivo = consulta["motivo_sefaz"]
                divergencia = consulta["divergencia_ncm"]
                descricao_erp = consulta.get("descricao_erp", "")
                ncm_erp = consulta["ncm_winthor"]
                ncm_sefaz = consulta["ncm_oficial"]
                cor_status = "sucesso" if status in {"949", "9490"} else "alerta"
                if status in {"GTIN_INVALIDO", "GTIN_FORA_GS1_BR"}:
                    cor_status = "erro"
                prefixo_produto = f"{codprod} | {descricao_erp} | " if descricao_erp else f"{codprod} | "
                self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | Sefaz {status}", cor_status))
                if motivo and status not in {"949", "9490"}:
                    self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | Motivo: {motivo}", "alerta"))
                self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | NCM ERP: {ncm_erp} | NCM GS1: {ncm_sefaz} | Divergencia: {divergencia}", cor_status))
                origem_cenarios = str(resultado.get("origem_cenarios", "atualizado") or "atualizado")
                if resultado["cenarios"]:
                    if origem_cenarios == "cache":
                        self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | {len(resultado['cenarios'])} cenario(s) tributario(s) mantido(s) do cache local para o NCM consultado.", "alerta"))
                    else:
                        self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | {len(resultado['cenarios'])} cenario(s) tributario(s) persistido(s) para o NCM consultado.", "sucesso"))
                else:
                    if origem_cenarios == "limpo":
                        self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | Nenhum cenario tributario encontrado para o NCM consultado; cache local atualizado para vazio.", "alerta"))
                    else:
                        self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | Nenhum cenario tributario persistido nesta rodada.", "alerta"))
                for warning in resultado["warnings"]:
                    self.enqueue("log", (f"{prefixo_produto}GTIN {gtin} | Aviso: {warning}", "alerta"))
                if self.classificacao_service.circuito_gtin_aberto() and not circuito_alertado:
                    self.enqueue("gtin_health", self.classificacao_service.verificar_saude_servico_gtin(force=False))
                    self.enqueue(
                        "log",
                        (
                            "Circuit breaker GTIN ativo: os proximos itens do lote seguirao apenas com reaproveitamento do cache oficial GS1 quando disponivel.",
                            "alerta",
                        ),
                    )
                    circuito_alertado = True

            self.enqueue("log", ("-" * 60, "info"))
            self.enqueue("log", ("CONCLUIDO! Base SQLite atualizada com consultas e cenarios tributarios.", "sucesso"))
            self.enqueue("finish", None)
        except Exception as erro:
            self.enqueue("log", (f"ERRO CRITICO: {erro}", "erro"))
            self.enqueue("enable_start", None)

    def run_ncm_sync(self, filtro_ncm: str = "", qtd_max: int | None = None) -> None:
        try:
            filtro_limpo = "".join(filter(str.isdigit, str(filtro_ncm or "")))
            self.enqueue("log", "Buscando NCMs no Winthor para consulta direta...")
            produtos_winthor = buscar_gtins_winthor(self.settings)
            if not produtos_winthor:
                self.enqueue("log", ("Nenhum produto encontrado ou erro no banco.", "alerta"))
                self.enqueue("enable_start", None)
                return

            ncms_unicos: list[str] = []
            vistos: set[str] = set()
            for produto in produtos_winthor:
                ncm = "".join(filter(str.isdigit, str(produto[2] or "")))
                if not ncm:
                    continue
                if filtro_limpo and filtro_limpo not in ncm:
                    continue
                if ncm in vistos:
                    continue
                vistos.add(ncm)
                ncms_unicos.append(ncm)

            if not ncms_unicos:
                self.enqueue("log", ("Nenhum NCM encontrado no ERP para o filtro informado.", "alerta"))
                self.enqueue("enable_start", None)
                return

            if not filtro_limpo:
                qtd_max = qtd_max if qtd_max is not None else 10
                ncms_unicos = ncms_unicos[: min(len(ncms_unicos), qtd_max)]

            self.enqueue("log", f"Iniciando consulta direta de {len(ncms_unicos)} NCM(s) do ERP...")
            total = max(len(ncms_unicos), 1)
            for i, ncm in enumerate(ncms_unicos):
                self.enqueue("progress", (i + 1) / total)
                resultado = self.classificacao_service.processar_ncm(ncm)
                origem_cenarios = str(resultado.get("origem_cenarios", "atualizado") or "atualizado")
                if resultado["cenarios"]:
                    if origem_cenarios == "cache":
                        self.enqueue("log", (f"NCM {ncm} | {len(resultado['cenarios'])} cenario(s) tributario(s) mantido(s) do cache local.", "alerta"))
                    else:
                        self.enqueue("log", (f"NCM {ncm} | {len(resultado['cenarios'])} cenario(s) tributario(s) persistido(s).", "sucesso"))
                else:
                    if origem_cenarios == "limpo":
                        self.enqueue("log", (f"NCM {ncm} | Nenhum cenario tributario encontrado; cache local atualizado para vazio.", "alerta"))
                    else:
                        self.enqueue("log", (f"NCM {ncm} | Nenhum cenario tributario persistido nesta rodada.", "alerta"))
                for warning in resultado["warnings"]:
                    self.enqueue("log", (f"NCM {ncm} | Aviso: {warning}", "alerta"))

            self.enqueue("log", ("-" * 60, "info"))
            self.enqueue("log", ("CONCLUIDO! Consulta direta de NCMs do ERP finalizada com sucesso.", "sucesso"))
            self.enqueue("finish", None)
        except Exception as erro:
            self.enqueue("log", (f"ERRO CRITICO: {erro}", "erro"))
            self.enqueue("enable_start", None)

    def run_anexo_sync(self) -> None:
        try:
            self.enqueue("log", "Consultando retorno de anexos no servico...")
            anexos = self.classificacao_service.anexo_service.sincronizar_anexos()
            if not anexos:
                self.enqueue("log", ("Servico de anexos retornou vazio; cache local preservado.", "alerta"))
                self.enqueue("finish", None)
                return
            self.repository.salvar_anexos_tributarios(anexos, substituir=True)
            total_especificidades = sum(len(anexo.get("especificidades", [])) for anexo in anexos)
            self.enqueue(
                "log",
                (
                    f"Servico de anexos atualizado com {len(anexos)} anexo(s) e {total_especificidades} especificidade(s).",
                    "sucesso",
                ),
            )
            self.enqueue("finish", None)
        except Exception as erro:
            self.enqueue("log", (f"ERRO CRITICO: {erro}", "erro"))
            self.enqueue("enable_start", None)

    def abrir_analise_ia(self, tipo_contexto: str, chave_contexto: str) -> None:
        if self.analise_window and self.analise_window.winfo_exists():
            self.analise_window.focus()
        else:
            self.analise_window = AnaliseIAWindow(self, self.ai_service)
        self.analise_window.carregar_analise(tipo_contexto, chave_contexto)
        self.analise_window.focus()

    def abrir_visualizador(self) -> None:
        if self.database_window and self.database_window.winfo_exists():
            self.database_window.focus()
            self.database_window.atualizar_tabela()
            return
        self.database_window = ConsultaDatabaseWindow(self, self.repository)
        self.database_window.focus()

    def abrir_visualizador_cenarios(self) -> None:
        if self.cenarios_window and self.cenarios_window.winfo_exists():
            self.cenarios_window.focus()
            self.cenarios_window.atualizar_tabela()
            return
        self.cenarios_window = CenariosTributariosWindow(self, self.repository)
        self.cenarios_window.focus()

    def abrir_visualizador_anexos(self) -> None:
        if self.anexos_window and self.anexos_window.winfo_exists():
            self.anexos_window.focus()
            self.anexos_window.atualizar_tabela()
            return
        self.anexos_window = AnexosTributariosWindow(self, self.repository)
        self.anexos_window.focus()

    def exportar_tudo(self) -> None:
        dados = self.repository.listar_consultas()
        if not dados:
            self.log("Nenhum registro salvo para exportar.")
            return
        caminho = exportar_consultas_excel(dados, prefixo="Exportacao_Completa_SQLite_GTIN")
        self.log(f"Exportacao concluida: {caminho}")
