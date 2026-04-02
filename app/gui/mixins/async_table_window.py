from __future__ import annotations

from queue import Empty, Queue
from threading import Thread

import customtkinter as ctk

from app.gui.table_utils import apply_column_filters, sort_records


class AsyncTableWindowMixin:
    render_batch_size = 300
    filter_columns_per_row = 4

    def _validacao_em_execucao(self) -> bool:
        return bool(getattr(self.master, "executando_validacao", False))

    def _estado_controle(self, carregando: bool) -> str:
        return "disabled" if carregando or self._validacao_em_execucao() else "normal"

    def _init_async_table_loader(self) -> None:
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self._table_request_id = 0
        self._table_render_token = 0
        self._table_render_items: list[dict] = []
        self._table_render_index = 0
        self._dados_carregados = False
        self.dados_base: list[dict] = []
        self.dados_atuais: list[dict] = []
        self.filter_entries: dict[str, ctk.CTkEntry] = {}
        self._sort_column = ""
        self._sort_reverse = False
        self._column_titles: dict[str, str] = {}
        self.after(100, self.processar_fila_ui)

    def _setup_column_filters(
        self,
        parent: ctk.CTkFrame,
        columns: list[tuple[str, str, int]],
        columns_per_row: int | None = None,
    ) -> None:
        self.column_defs = list(columns)
        self._column_titles = {key: title for key, title, _ in self.column_defs}
        self.filter_entries = {}

        max_columns = max(columns_per_row or self.filter_columns_per_row, 1)
        for idx, (key, title, _) in enumerate(self.column_defs):
            row = idx // max_columns
            column = idx % max_columns
            parent.grid_columnconfigure(column, weight=1)
            frame = ctk.CTkFrame(parent, fg_color="transparent")
            frame.grid(row=row, column=column, padx=6, pady=8, sticky="ew")
            ctk.CTkLabel(frame, text=title).pack(anchor="w")
            entry = ctk.CTkEntry(frame)
            entry.pack(fill="x", pady=(4, 0))
            entry.bind("<KeyRelease>", lambda _event: self._disparar_busca_programada())
            self.filter_entries[key] = entry

    def _configurar_treeview_ordenavel(self) -> None:
        for key, title, _ in getattr(self, "column_defs", []):
            self.tree.heading(key, text=title, command=lambda coluna=key: self._alternar_ordenacao(coluna))
        self._atualizar_cabecalhos_ordenacao()

    def _atualizar_cabecalhos_ordenacao(self) -> None:
        for key, title, _ in getattr(self, "column_defs", []):
            suffix = ""
            if key == self._sort_column:
                suffix = " ▼" if self._sort_reverse else " ▲"
            self.tree.heading(key, text=f"{title}{suffix}", command=lambda coluna=key: self._alternar_ordenacao(coluna))

    def _alternar_ordenacao(self, column: str) -> None:
        if self._sort_column == column:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = column
            self._sort_reverse = False
        self._atualizar_cabecalhos_ordenacao()
        self._aplicar_filtros_e_ordenacao()

    def _obter_filtros(self) -> dict[str, str]:
        return {key: entry.get() for key, entry in self.filter_entries.items()}

    def _disparar_busca_programada(self) -> None:
        if hasattr(self, "_timer_busca"):
            self.after_cancel(self._timer_busca)
        self._timer_busca = self.after(300, self.atualizar_tabela)

    def limpar_filtros(self) -> None:
        for entry in self.filter_entries.values():
            entry.delete(0, "end")
        self.atualizar_tabela()

    def atualizar_tabela(self, recarregar: bool = False) -> None:
        if recarregar or not self._dados_carregados:
            self._solicitar_carga_tabela()
            return
        self._aplicar_filtros_e_ordenacao()

    def _solicitar_carga_tabela(self) -> None:
        self._table_request_id += 1
        request_id = self._table_request_id
        self._configurar_estado_tabela(True)
        self.lbl_status.configure(text="Carregando dados...")
        Thread(target=self._worker_carregar_tabela, args=(request_id,), daemon=True).start()

    def _worker_carregar_tabela(self, request_id: int) -> None:
        try:
            dados = self._buscar_dados_tabela()
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
                    self.dados_base = list(dados.get("dados", []))
                    self._dados_carregados = True
                    self._aplicar_filtros_e_ordenacao()
                elif action == "erro_tabela":
                    dados = dict(payload)
                    if int(dados.get("request_id", 0)) != self._table_request_id:
                        continue
                    self._configurar_estado_tabela(False)
                    self.lbl_status.configure(text=f"Erro ao carregar dados: {dados.get('erro', '')}")
        except Empty:
            pass
        self.after(100, self.processar_fila_ui)

    def _aplicar_filtros_e_ordenacao(self) -> None:
        filtros = self._obter_filtros()
        dados = apply_column_filters(self.dados_base, filtros)
        if self._sort_column:
            dados = sort_records(dados, self._sort_column, self._sort_reverse)
        self.dados_atuais = dados
        self.tree.delete(*self.tree.get_children())
        self._table_render_token += 1
        self._table_render_items = list(self.dados_atuais)
        self._table_render_index = 0
        self._renderizar_lote_tabela(self._table_render_token)

    def _renderizar_lote_tabela(self, render_token: int) -> None:
        if render_token != self._table_render_token:
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
            self.after(1, lambda token=render_token: self._renderizar_lote_tabela(token))
            return

        self._configurar_estado_tabela(False)
        self.lbl_status.configure(text=self._texto_status_concluido(total))

    def _buscar_dados_tabela(self) -> list[dict]:
        raise NotImplementedError

    def _inserir_linha_tabela(self, item: dict) -> None:
        raise NotImplementedError

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} registro(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        return None
