from __future__ import annotations

import os
from datetime import datetime
from queue import Empty, Queue
from threading import Thread
from tkinter import ttk

import customtkinter as ctk

from app.config import SQLITE_DB_PATH, load_settings
from app.services.classificacao_tributaria_service import ClassificacaoTributariaService
from app.services.oracle_service import buscar_gtins_winthor
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository
from app.utils.input_utils import parse_positive_int


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


COLUNAS_TABELA = [
    ("gtin", "GTIN", 130),
    ("cod_winthor", "Cod. Winthor", 110),
    ("status_sefaz", "Status Sefaz", 110),
    ("ncm_winthor", "NCM Winthor", 110),
    ("ncm_oficial", "NCM GS1", 110),
    ("divergencia_ncm", "Divergencia", 220),
    ("descricao_produto", "Descricao", 240),
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
    ("codigo_especificidade", "Cod. Especificidade", 150),
    ("descricao_especificidade", "Descricao Especificidade", 260),
    ("valor", "Valor", 110),
    ("tipo", "Tipo", 110),
    ("especificidade_publicacao", "Pub. Especificidade", 150),
    ("especificidade_inicio_vigencia", "Inicio Vig. Espec.", 150),
    ("ultima_atualizacao", "Atualizado Em", 140),
]


class ConsultaDatabaseWindow(ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Consultas GTIN")
        self.geometry("1380x720")
        self.minsize(1100, 600)
        self.dados_atuais: list[dict] = []

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(
            self,
            text="Consultas persistidas em SQLite",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")

        self.filtro_frame = ctk.CTkFrame(self)
        self.filtro_frame.grid(row=1, column=0, padx=20, pady=(0, 10), sticky="ew")
        for coluna in range(5):
            self.filtro_frame.grid_columnconfigure(coluna, weight=1)

        self.entry_gtin = self._criar_filtro("GTIN", 0)
        self.entry_status = self._criar_filtro("Status", 1)
        self.entry_divergencia = self._criar_filtro("Divergencia", 2)
        self.entry_ncm = self._criar_filtro("NCM", 3)
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
        self.btn_revalidar = ctk.CTkButton(
            self.actions_frame,
            text="Revalidar Selecionados",
            command=self.revalidar_selecionados,
            fg_color="#3498db",
            hover_color="#2980b9",
        )
        self.btn_revalidar.pack(side="left")
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
            "gtin": self.entry_gtin.get(),
            "status_sefaz": self.entry_status.get(),
            "divergencia_ncm": self.entry_divergencia.get(),
            "ncm": self.entry_ncm.get(),
            "descricao_produto": self.entry_descricao.get(),
        }

    def atualizar_tabela(self) -> None:
        self.dados_atuais = self.repository.listar_consultas(self._obter_filtros())
        self.tree.delete(*self.tree.get_children())
        for item in self.dados_atuais:
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
        self.lbl_status.configure(text=f"{len(self.dados_atuais)} registro(s)")
        self.btn_revalidar.configure(state="disabled" if self.master.executando_validacao else "normal")

    def limpar_filtros(self) -> None:
        for entry in [self.entry_gtin, self.entry_status, self.entry_divergencia, self.entry_ncm, self.entry_descricao]:
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
        for item_id in selecionados_ids:
            valores = self.tree.item(item_id, "values")
            produtos_para_revalidar.append((valores[1], valores[0], valores[3]))
        self.master.produtos_selecionados = produtos_para_revalidar
        self.master.atualizar_resumo_selecao()
        self.master.start_validation_thread()


class CenariosTributariosWindow(ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Cenarios Tributarios por NCM")
        self.geometry("1460x760")
        self.minsize(1180, 620)
        self.dados_atuais: list[dict] = []

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
        self.btn_consultar_erp.pack(side="left")
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
        self.dados_atuais = self.repository.listar_cenarios_tributarios(self._obter_filtros())
        self.tree.delete(*self.tree.get_children())
        for item in self.dados_atuais:
            self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in COLUNAS_CENARIOS])
        self.lbl_status.configure(text=f"{len(self.dados_atuais)} cenario(s)")

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

class AnexosTributariosWindow(ctk.CTkToplevel):
    def __init__(self, master, repository: ConsultaGtinRepository):
        super().__init__(master)
        self.repository = repository
        self.title("Base SQLite - Retorno dos Anexos")
        self.geometry("1500x760")
        self.minsize(1200, 620)
        self.dados_atuais: list[dict] = []

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
        self.dados_atuais = self.repository.listar_retorno_anexos(self._obter_filtros())
        self.tree.delete(*self.tree.get_children())
        for item in self.dados_atuais:
            self.tree.insert("", "end", values=[item.get(chave, "") for chave, _, _ in COLUNAS_ANEXOS])
        self.lbl_status.configure(text=f"{len(self.dados_atuais)} linha(s)")

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
        self.geometry("1280x720")
        self.minsize(1080, 580)

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
            columns=["codprod", "gtin", "ncm", "status_consulta"],
            show="headings",
            selectmode="extended",
        )
        self.tree.heading("codprod", text="Cod. Winthor")
        self.tree.heading("gtin", text="GTIN")
        self.tree.heading("ncm", text="NCM")
        self.tree.heading("status_consulta", text="Status da Consulta")
        self.tree.column("codprod", width=140, anchor="w")
        self.tree.column("gtin", width=180, anchor="w")
        self.tree.column("ncm", width=160, anchor="w")
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
                self.tree.insert("", "end", iid=f"{codprod}|{gtin_valor}", values=(codprod, gtin_valor, ncm_valor, status_consulta))
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
        self.database_window: ConsultaDatabaseWindow | None = None
        self.cenarios_window: CenariosTributariosWindow | None = None
        self.anexos_window: AnexosTributariosWindow | None = None
        self.selecao_window: SelecaoGtinsWindow | None = None
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
        for botao in [self.btn_iniciar, self.btn_selecionar, self.btn_limpar_selecao, self.btn_visualizar, self.btn_visualizar_cenarios, self.btn_visualizar_anexos, self.btn_exportar]:
            botao.configure(state=estado)

    def log(self, text: str, level: str = "info") -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.textbox.insert("end", f"[{timestamp}] ", level)
        self.textbox.insert("end", f"{text}\n")
        self.textbox.see("end")

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

            total = max(len(produtos), 1)
            for i, produto in enumerate(produtos):
                self.enqueue("progress", (i + 1) / total)
                resultado = self.classificacao_service.processar_produto(produto)
                consulta = resultado["consulta"]
                gtin = consulta["gtin"]
                codprod = consulta["cod_winthor"]
                status = consulta["status_sefaz"]
                divergencia = consulta["divergencia_ncm"]
                ncm_erp = consulta["ncm_winthor"]
                ncm_sefaz = consulta["ncm_oficial"]
                cor_status = "sucesso" if status in {"949", "9490"} else "alerta"
                if status in {"GTIN_INVALIDO", "GTIN_FORA_GS1_BR"}:
                    cor_status = "erro"
                self.enqueue("log", (f"{codprod} | GTIN {gtin} | Sefaz {status}", cor_status))
                self.enqueue("log", (f"{codprod} | GTIN {gtin} | NCM ERP: {ncm_erp} | NCM GS1: {ncm_sefaz} | Divergencia: {divergencia}", cor_status))
                if resultado["cenarios"]:
                    self.enqueue("log", (f"{codprod} | GTIN {gtin} | {len(resultado['cenarios'])} cenario(s) tributario(s) persistido(s) para o NCM consultado.", "sucesso"))
                else:
                    self.enqueue("log", (f"{codprod} | GTIN {gtin} | Nenhum cenario tributario persistido nesta rodada.", "alerta"))
                for warning in resultado["warnings"]:
                    self.enqueue("log", (f"{codprod} | GTIN {gtin} | Aviso: {warning}", "alerta"))

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
                if resultado["cenarios"]:
                    self.enqueue("log", (f"NCM {ncm} | {len(resultado['cenarios'])} cenario(s) tributario(s) persistido(s).", "sucesso"))
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
