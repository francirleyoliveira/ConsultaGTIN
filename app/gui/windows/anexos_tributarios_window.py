from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

from app.gui.constants import COLUNAS_ANEXOS
from app.gui.mixins.async_table_window import AsyncTableWindowMixin
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository


class AnexosTributariosWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.column_defs = COLUNAS_ANEXOS
        self.title("Base SQLite - Retorno dos Anexos")
        self.geometry("1500x760")
        self.minsize(1200, 620)
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
        self._setup_column_filters(self.filtro_frame, self.column_defs, columns_per_row=4)

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
        return self.repository.listar_retorno_anexos({})

    def _inserir_linha_tabela(self, item: dict) -> None:
        self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in self.column_defs])

    def _texto_status_concluido(self, total: int) -> str:
        return f"{total} linha(s)"

    def _configurar_estado_tabela(self, carregando: bool) -> None:
        estado = self._estado_controle(carregando)
        self.btn_atualizar_servico.configure(state=estado)

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum anexo para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_Retorno_Anexos")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def atualizar_do_servico(self) -> None:
        self.master.start_anexo_sync_thread()
