from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

from app.gui.constants import COLUNAS_CENARIOS
from app.gui.mixins.async_table_window import AsyncTableWindowMixin
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository


class CenariosTributariosWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.column_defs = COLUNAS_CENARIOS
        self.title("Base SQLite - Cenarios Tributarios por NCM")
        self.geometry("1460x760")
        self.minsize(1180, 620)
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
        self._setup_column_filters(self.filtro_frame, self.column_defs, columns_per_row=5)

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

        self.tree = ttk.Treeview(self.table_frame, columns=[c[0] for c in self.column_defs], show="headings")
        for chave, _, largura in self.column_defs:
            self.tree.column(chave, width=largura, anchor="w", stretch=True)
        self._configurar_treeview_ordenavel()

        scroll_y = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        self.after(100, lambda: self.atualizar_tabela(recarregar=True))

    def _buscar_dados_tabela(self) -> list[dict]:
        return self.repository.listar_cenarios_tributarios({})

    def _inserir_linha_tabela(self, item: dict) -> None:
        self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in self.column_defs])

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} cenario(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        estado = self._estado_controle(carregando)
        self.btn_consultar_erp.configure(state=estado)
        self.btn_analise_ia.configure(state=estado)

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum cenario para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_Cenarios_Tributarios")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def consultar_ncms_erp(self) -> None:
        filtro_ncm = str(self.filter_entries.get("ncm").get() if self.filter_entries.get("ncm") else "").strip()
        self.master.start_ncm_sync_thread(filtro_ncm)

    def analisar_ncm_com_ia(self) -> None:
        selecionados_ids = self.tree.selection()
        if len(selecionados_ids) != 1:
            self.lbl_status.configure(text="Selecione exatamente um cenario para analisar o NCM com IA")
            return
        indices = {chave: idx for idx, (chave, _, _) in enumerate(self.column_defs)}
        valores = self.tree.item(selecionados_ids[0], "values")
        ncm = str(valores[indices["ncm"]] or "").strip()
        if not ncm:
            self.lbl_status.configure(text="NCM invalido para analise")
            return
        self.master.abrir_analise_ia("NCM", ncm)
