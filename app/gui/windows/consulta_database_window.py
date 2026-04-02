from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

from app.gui.constants import COLUNAS_TABELA
from app.gui.mixins.async_table_window import AsyncTableWindowMixin
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository


class ConsultaDatabaseWindow(AsyncTableWindowMixin, ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.column_defs = COLUNAS_TABELA
        self.title("Base SQLite - Consultas GTIN")
        self.geometry("1380x720")
        self.minsize(1100, 600)
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

        self.tree = ttk.Treeview(self.table_frame, columns=[c[0] for c in self.column_defs], show="headings")
        for chave, _, largura in self.column_defs:
            self.tree.column(chave, width=largura, anchor="w", stretch=True)
        self._configurar_treeview_ordenavel()

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

        self.after(100, lambda: self.atualizar_tabela(recarregar=True))

    def _buscar_dados_tabela(self) -> list[dict]:
        return self.repository.listar_consultas({})

    def _inserir_linha_tabela(self, item: dict) -> None:
        valores = [item.get(chave, "") for chave, _, _ in self.column_defs]
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
        estado = self._estado_controle(carregando)
        self.btn_revalidar.configure(state=estado)
        self.btn_analise_ia.configure(state=estado)

    def exportar_resultado(self) -> None:
        if not self.dados_atuais:
            self.lbl_status.configure(text="Nenhum registro para exportar")
            return
        caminho = exportar_consultas_excel(self.dados_atuais, prefixo="Exportacao_SQLite_GTIN")
        self.lbl_status.configure(text=f"Exportado: {caminho.name}")

    def revalidar_selecionados(self) -> None:
        if self._validacao_em_execucao():
            self.lbl_status.configure(text="Aguarde a validacao atual terminar")
            return
        selecionados_ids = self.tree.selection()
        if not selecionados_ids:
            self.lbl_status.configure(text="Selecione itens na tabela primeiro")
            return
        produtos_para_revalidar = []
        indices = {chave: idx for idx, (chave, _, _) in enumerate(self.column_defs)}
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
        indices = {chave: idx for idx, (chave, _, _) in enumerate(self.column_defs)}
        valores = self.tree.item(selecionados_ids[0], "values")
        gtin = str(valores[indices["gtin"]] or "").strip()
        if not gtin:
            self.lbl_status.configure(text="GTIN invalido para analise")
            return
        self.master.abrir_analise_ia("GTIN", gtin)
