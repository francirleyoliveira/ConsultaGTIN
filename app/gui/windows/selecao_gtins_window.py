from __future__ import annotations

from queue import Empty, Queue
from threading import Thread
from tkinter import ttk

import customtkinter as ctk

from app.gui.constants import COLUNAS_SELECAO_GTINS
from app.gui.table_utils import apply_column_filters, sort_records
from app.services.oracle_service import buscar_gtins_winthor
from app.services.sqlite_service import ConsultaGtinRepository


class SelecaoGtinsWindow(ctk.CTkToplevel):
    def __init__(self, master, settings, repository: ConsultaGtinRepository, on_confirmar):
        super().__init__(master)
        self.settings = settings
        self.repository = repository
        self.on_confirmar = on_confirmar
        self.ui_queue: Queue[tuple[str, object]] = Queue()
        self.todos_registros: list[dict] = []
        self.registros_visiveis: list[dict] = []
        self.status_por_gtin: dict[str, dict[str, str]] = {}
        self.filter_entries: dict[str, ctk.CTkEntry] = {}
        self._sort_column = ""
        self._sort_reverse = False

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
        self._criar_filtros_colunas()

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")

        self.btn_carregar = ctk.CTkButton(self.actions_frame, text="Carregar GTINs", command=self.carregar_produtos)
        self.btn_carregar.pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Aplicar Filtros", command=self.aplicar_filtros).pack(side="left", padx=(0, 8))
        ctk.CTkButton(self.actions_frame, text="Limpar", command=self.limpar_filtros).pack(side="left", padx=(0, 8))
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
            columns=[coluna[0] for coluna in COLUNAS_SELECAO_GTINS],
            show="headings",
            selectmode="extended",
        )
        for chave, titulo, largura in COLUNAS_SELECAO_GTINS:
            self.tree.heading(chave, text=titulo, command=lambda coluna=chave: self._alternar_ordenacao(coluna))
            self.tree.column(chave, width=largura, anchor="w", stretch=True)

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self._atualizar_cabecalhos_ordenacao()
        self.after(100, self.processar_fila_ui)
        self.lbl_status.configure(text="Clique em Carregar GTINs para iniciar")

    def _criar_filtros_colunas(self) -> None:
        columns_per_row = 3
        for idx, (key, title, _) in enumerate(COLUNAS_SELECAO_GTINS):
            row = idx // columns_per_row
            column = idx % columns_per_row
            self.filtro_frame.grid_columnconfigure(column, weight=1)
            frame = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
            frame.grid(row=row, column=column, padx=6, pady=8, sticky="ew")
            ctk.CTkLabel(frame, text=title).pack(anchor="w")
            entry = ctk.CTkEntry(frame)
            entry.pack(fill="x", pady=(4, 0))
            entry.bind("<KeyRelease>", lambda _event: self._disparar_busca_programada())
            self.filter_entries[key] = entry

        frame_limite = ctk.CTkFrame(self.filtro_frame, fg_color="transparent")
        frame_limite.grid(row=1, column=2, padx=6, pady=8, sticky="ew")
        ctk.CTkLabel(frame_limite, text="Maximo na grade").pack(anchor="w")
        self.entry_limite = ctk.CTkEntry(frame_limite)
        self.entry_limite.insert(0, "500")
        self.entry_limite.pack(fill="x", pady=(4, 0))
        self.entry_limite.bind("<Return>", lambda _event: self.aplicar_filtros())

    def _atualizar_cabecalhos_ordenacao(self) -> None:
        for key, title, _ in COLUNAS_SELECAO_GTINS:
            suffix = ""
            if key == self._sort_column:
                suffix = " ▼" if self._sort_reverse else " ▲"
            self.tree.heading(key, text=f"{title}{suffix}", command=lambda coluna=key: self._alternar_ordenacao(coluna))

    def _alternar_ordenacao(self, coluna: str) -> None:
        if self._sort_column == coluna:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = coluna
            self._sort_reverse = False
        self._atualizar_cabecalhos_ordenacao()
        self.aplicar_filtros()

    def _disparar_busca_programada(self) -> None:
        if hasattr(self, "_timer_busca"):
            self.after_cancel(self._timer_busca)
        self._timer_busca = self.after(300, self.aplicar_filtros)

    def _obter_filtros(self) -> dict[str, str]:
        return {key: entry.get() for key, entry in self.filter_entries.items()}

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
                    self.status_por_gtin = dict(dados.get("status_por_gtin", {}))
                    self.todos_registros = self._normalizar_produtos(list(dados.get("produtos", [])))
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

    def _normalizar_produtos(self, produtos: list[tuple]) -> list[dict]:
        registros: list[dict] = []
        for produto in produtos:
            codprod = produto[0]
            gtin_valor = str(produto[1] or "")
            ncm_valor = str(produto[2] or "")
            descricao_erp = str(produto[3] or "") if len(produto) > 3 else ""
            registros.append(
                {
                    "iid": f"{codprod}|{gtin_valor}",
                    "codprod": str(codprod),
                    "gtin": gtin_valor,
                    "ncm": ncm_valor,
                    "descricao_erp": descricao_erp,
                    "status_consulta": self._montar_status_consulta(gtin_valor),
                    "_produto": produto,
                }
            )
        return registros

    def _obter_limite(self) -> int:
        try:
            limite = int(self.entry_limite.get().strip() or "500")
            return max(1, min(limite, 5000))
        except ValueError:
            self.entry_limite.delete(0, "end")
            self.entry_limite.insert(0, "500")
            return 500

    def aplicar_filtros(self) -> None:
        registros = apply_column_filters(self.todos_registros, self._obter_filtros())
        if self._sort_column:
            registros = sort_records(registros, self._sort_column, self._sort_reverse)

        total_filtrado = len(registros)
        limite = self._obter_limite()
        self.registros_visiveis = registros[:limite]
        self.tree.delete(*self.tree.get_children())
        for registro in self.registros_visiveis:
            self.tree.insert(
                "",
                "end",
                iid=registro["iid"],
                values=[registro.get(chave, "") for chave, _, _ in COLUNAS_SELECAO_GTINS],
            )

        if total_filtrado > limite:
            self.lbl_status.configure(text=f"{total_filtrado} item(ns) encontrado(s), exibindo os primeiros {limite}")
        else:
            self.lbl_status.configure(text=f"{total_filtrado} item(ns) encontrado(s)")

    def limpar_filtros(self) -> None:
        for entry in self.filter_entries.values():
            entry.delete(0, "end")
        self.aplicar_filtros()

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
        for registro in self.registros_visiveis:
            if registro["iid"] in ids:
                selecionados.append(registro["_produto"])
        if not selecionados:
            self.lbl_status.configure(text="Selecione ao menos um GTIN")
            return
        self.on_confirmar(selecionados)
        self.destroy()
