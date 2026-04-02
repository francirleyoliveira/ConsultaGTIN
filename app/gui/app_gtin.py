from __future__ import annotations

import os
from datetime import datetime
from queue import Empty, Queue
from threading import Thread

import customtkinter as ctk

from app.config import SQLITE_DB_PATH, load_settings
from app.gui.windows.analise_ia_window import AnaliseIAWindow
from app.gui.windows.anexos_tributarios_window import AnexosTributariosWindow
from app.gui.windows.cenarios_tributarios_window import CenariosTributariosWindow
from app.gui.windows.consulta_database_window import ConsultaDatabaseWindow
from app.gui.windows.selecao_gtins_window import SelecaoGtinsWindow
from app.services.ai_classificacao_service import AIClassificationService
from app.services.classificacao_tributaria_service import ClassificacaoTributariaService
from app.services.oracle_service import buscar_gtins_winthor
from app.services.relatorio_service import exportar_consultas_excel
from app.services.sqlite_service import ConsultaGtinRepository
from app.utils.input_utils import parse_positive_int
from app.validators.gtin import validar_digito_gtin, validar_prefixo_gs1_brasil

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

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
                        self.database_window.atualizar_tabela(recarregar=True)
                    if self.cenarios_window and self.cenarios_window.winfo_exists():
                        self.cenarios_window.atualizar_tabela(recarregar=True)
                    if self.anexos_window and self.anexos_window.winfo_exists():
                        self.anexos_window.atualizar_tabela(recarregar=True)
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
            self.database_window.atualizar_tabela(recarregar=True)
            return
        self.database_window = ConsultaDatabaseWindow(self, self.repository)
        self.database_window.focus()

    def abrir_visualizador_cenarios(self) -> None:
        if self.cenarios_window and self.cenarios_window.winfo_exists():
            self.cenarios_window.focus()
            self.cenarios_window.atualizar_tabela(recarregar=True)
            return
        self.cenarios_window = CenariosTributariosWindow(self, self.repository)
        self.cenarios_window.focus()

    def abrir_visualizador_anexos(self) -> None:
        if self.anexos_window and self.anexos_window.winfo_exists():
            self.anexos_window.focus()
            self.anexos_window.atualizar_tabela(recarregar=True)
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
