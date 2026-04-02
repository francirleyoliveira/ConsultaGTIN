from __future__ import annotations

from queue import Empty, Queue
from threading import Thread

import customtkinter as ctk

from app.services.ai_classificacao_service import AIClassificationService

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
