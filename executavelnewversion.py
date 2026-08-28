from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from executavel import (
    COLUNAS_DATA,
    OPTIONAL_INPUT_DEFAULTS,
    RENAME_MAP,
    colunas_thproc_regular,
    conectar_ao_mysql,
    listar_carteiras,
)


COLUNAS_INSERIVEIS = list(dict.fromkeys(colunas_thproc_regular))
USUARIO_SUBMIT_ID_PADRAO = 18
MAPEAMENTO_COLUNAS_PLANILHA = {
    **RENAME_MAP,
    "Cliente": "cliente",
    "Tipo Cliente": "tipoPoloCliente",
    "Corresponsável": "corresponsavel",
    "Tipo Evento": "tipoEvento",
    "Solicitante do Andamento": "solicitanteAndamento",
    "Respons\u00e1vel do Andamento": "responsavelAndamento",
    "Correspons\u00e1vel do Andamento": "corresponsavelAndamento",
    "Prioridade De": "prioridadeDe",
    "Solicitante Evento": "solicitanteEvento",
    "Respons\u00e1vel Evento": "responsavelEvento",
}
COLUNAS_AUTH_USER_FULLNAME = [
    "solicitanteAndamento",
    "responsavelAndamento",
    "corresponsavelAndamento",
    "prioridadeDe",
    "solicitanteEvento",
    "responsavelEvento",
]
DEFAULTS_FLUXO_REGULAR = {
    **OPTIONAL_INPUT_DEFAULTS,
    "verificado": 0,
    "exportado": 0,
    "excluido": 0,
}
TIPOS_POLO_CLIENTE_VALIDOS = {
    "ativo": "Ativo",
    "passivo": "Passivo",
}


@dataclass
class AppState:
    path: Optional[str] = None
    sheet_name: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    insert_columns: List[str] = None
    carteira_id: Optional[int] = None
    carteira_nome: str = ""
    clientes_sem_grupo: List[str] = None
    criticas: List[str] = None
    avisos: List[str] = None

    def __post_init__(self):
        if self.insert_columns is None:
            self.insert_columns = []
        if self.clientes_sem_grupo is None:
            self.clientes_sem_grupo = []
        if self.criticas is None:
            self.criticas = []
        if self.avisos is None:
            self.avisos = []


def normalizar_valor_sql(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()
    if isinstance(valor, np.generic):
        return valor.item()
    return valor


def normalizar_valor_coluna_sql(coluna: str, valor):
    valor = normalizar_valor_sql(valor)
    if coluna == "natureza" and isinstance(valor, str):
        return valor.rstrip()
    return valor


def montar_registros_crus(df: pd.DataFrame, colunas: List[str]) -> List[Tuple]:
    registros: List[Tuple] = []
    for row in df[colunas].itertuples(index=False, name=None):
        registros.append(tuple(normalizar_valor_coluna_sql(coluna, valor) for coluna, valor in zip(colunas, row)))
    return registros


def preparar_campos_submit(df: pd.DataFrame, colunas: List[str]) -> tuple[pd.DataFrame, List[str]]:
    df_envio = formatar_datas_para_envio(df)
    colunas_envio = list(colunas)

    if "data_hora_submit" in COLUNAS_INSERIVEIS:
        df_envio["data_hora_submit"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "data_hora_submit" not in colunas_envio:
            colunas_envio.append("data_hora_submit")

    if "usuario_submit_id" in COLUNAS_INSERIVEIS:
        df_envio["usuario_submit_id"] = USUARIO_SUBMIT_ID_PADRAO
        if "usuario_submit_id" not in colunas_envio:
            colunas_envio.append("usuario_submit_id")

    return df_envio, colunas_envio


def formatar_datas_para_envio(df: pd.DataFrame) -> pd.DataFrame:
    df_formatado = df.copy()

    for coluna in COLUNAS_DATA:
        if coluna not in df_formatado.columns:
            continue

        serie = pd.to_datetime(df_formatado[coluna], errors="coerce", dayfirst=True)
        df_formatado[coluna] = pd.Series(pd.NA, index=df_formatado.index, dtype="string")
        mascara_valida = serie.notna()
        df_formatado.loc[mascara_valida, coluna] = serie.loc[mascara_valida].dt.strftime("%Y-%m-%d")

    return df_formatado


def normalizar_vazios_para_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for coluna in df.columns:
        serie = df[coluna]
        if pd.api.types.is_object_dtype(serie) or pd.api.types.is_string_dtype(serie):
            texto = serie.astype("string")
            mascara_vazia = texto.str.strip().eq("")
            df[coluna] = serie.mask(mascara_vazia, pd.NA)
    return df


def preparar_dataframe_para_insert(df: pd.DataFrame) -> pd.DataFrame:
    df = normalizar_vazios_para_null(df.copy())

    if "cnj" in df.columns:
        df["cnj"] = df["cnj"].astype("string").str.strip().replace({"": pd.NA})
    if "natureza" in df.columns:
        mascara_preenchida = df["natureza"].notna()
        df.loc[mascara_preenchida, "natureza"] = df.loc[mascara_preenchida, "natureza"].astype(str).str.rstrip()

    for coluna in COLUNAS_INSERIVEIS:
        if coluna not in df.columns:
            df[coluna] = DEFAULTS_FLUXO_REGULAR.get(coluna)

    return df


def filtrar_linhas_sem_cnj(df: pd.DataFrame) -> tuple[pd.DataFrame, List[int]]:
    if "cnj" not in df.columns:
        return df.copy(), []

    serie = df["cnj"].astype("string").str.strip()
    mascara_preenchida = serie.notna() & ~serie.eq("")
    if "natureza" in df.columns:
        natureza = df["natureza"].fillna("").astype(str).str.strip()
        mascara_administrativa = natureza.eq("Administrativa")
    else:
        mascara_administrativa = pd.Series(False, index=df.index)
    mascara_valida = mascara_preenchida | mascara_administrativa
    linhas_ignoradas = (df.index[~mascara_valida] + 2).tolist()
    return df.loc[mascara_valida].copy(), linhas_ignoradas


def linhas_administrativas_sem_cnj(df: pd.DataFrame) -> List[int]:
    if "cnj" not in df.columns or "natureza" not in df.columns:
        return []

    cnj = df["cnj"].astype("string").str.strip()
    natureza = df["natureza"].fillna("").astype(str).str.strip()
    mascara_invalida = natureza.eq("Administrativa") & (cnj.isna() | cnj.eq(""))
    return (df.index[mascara_invalida] + 2).tolist()


def validar_tipo_polo_cliente(df: pd.DataFrame) -> tuple[pd.DataFrame, bool, List[int], List[str]]:
    if "tipoPoloCliente" not in df.columns:
        return df.copy(), False, [], []

    df_validado = df.copy()
    serie = df_validado["tipoPoloCliente"].astype("string").str.strip()
    serie_normalizada = serie.str.lower()
    mascara_preenchida = serie.notna() & ~serie.eq("")
    mascara_valida = mascara_preenchida & serie_normalizada.isin(TIPOS_POLO_CLIENTE_VALIDOS)

    df_validado["tipoPoloCliente"] = serie_normalizada.map(TIPOS_POLO_CLIENTE_VALIDOS)

    linhas_invalidas = (df_validado.index[~mascara_valida] + 2).tolist()
    valores_invalidos = sorted(
        serie[~mascara_valida].fillna("<vazio>").replace("", "<vazio>").unique().tolist()
    )
    return df_validado, len(linhas_invalidas) == 0, linhas_invalidas, valores_invalidos


def normalizar_colunas_planilha(df: pd.DataFrame) -> pd.DataFrame:
    renomear = {}
    for origem, destino in MAPEAMENTO_COLUNAS_PLANILHA.items():
        if origem in df.columns and destino not in df.columns:
            renomear[origem] = destino

    if not renomear:
        return df

    return df.rename(columns=renomear)


def aplicar_carteira_e_codlote(df: pd.DataFrame, carteira_id: int, carteira_nome: str) -> pd.DataFrame:
    df = df.copy()
    df["carteira"] = pd.Series([carteira_id] * len(df), index=df.index, dtype="Int64")
    df["codlote"] = f"{carteira_nome} {datetime.now().strftime('%d/%m/%Y')}"
    return df


def buscar_nomegrupo_ids_por_cliente(clientes: pd.Series) -> tuple[dict[str, Optional[int]], List[str]]:
    valores = []
    vistos = set()

    for valor in clientes.fillna("").astype(str):
        cliente = valor.strip()
        if cliente and cliente not in vistos:
            vistos.add(cliente)
            valores.append(cliente)

    if not valores:
        return {}, []

    conn = None
    cur = None
    resultado: dict[str, Optional[int]] = {}
    clientes_sem_grupo: List[str] = []

    try:
        conn, cur = conectar_ao_mysql()
        sql = (
            "SELECT t.grupo_id "
            "FROM thcliente t "
            "WHERE t.name_cli LIKE %s "
            "ORDER BY t.id DESC "
            "LIMIT 1"
        )

        for cliente in valores:
            grupo_id = None
            for padrao in (f"% {cliente}%", f"%{cliente}%"):
                cur.execute(sql, (padrao,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    grupo_id = int(row[0])
                    break
            resultado[cliente] = grupo_id
            if grupo_id is None:
                clientes_sem_grupo.append(cliente)

        return resultado, clientes_sem_grupo
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def buscar_ids_por_fullname(valores_coluna: pd.Series) -> tuple[dict[str, Optional[int]], List[str]]:
    valores = []
    vistos = set()

    for valor in valores_coluna.fillna("").astype(str):
        nome = valor.strip()
        if nome and nome not in vistos:
            vistos.add(nome)
            valores.append(nome)

    if not valores:
        return {}, []

    conn = None
    cur = None
    resultado: dict[str, Optional[int]] = {}
    nomes_nao_encontrados: List[str] = []

    try:
        conn, cur = conectar_ao_mysql()
        sql = (
            "SELECT u.id "
            "FROM auth_user u "
            "WHERE u.fullname LIKE %s "
            "ORDER BY u.id DESC "
            "LIMIT 1"
        )

        for nome in valores:
            cliente_id = None
            for padrao in (f"% {nome}%", f"%{nome}%"):
                cur.execute(sql, (padrao,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    cliente_id = int(row[0])
                    break
            resultado[nome] = cliente_id
            if cliente_id is None:
                nomes_nao_encontrados.append(nome)

        return resultado, nomes_nao_encontrados
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def buscar_ids_corresponsavel(valores_coluna: pd.Series) -> tuple[dict[str, Optional[int]], List[str]]:
    valores = []
    vistos = set()

    for valor in valores_coluna.fillna("").astype(str):
        nome = valor.strip()
        if nome and nome not in vistos:
            vistos.add(nome)
            valores.append(nome)

    if not valores:
        return {}, []

    conn = None
    cur = None
    resultado: dict[str, Optional[int]] = {}
    nomes_nao_encontrados: List[str] = []

    try:
        conn, cur = conectar_ao_mysql()
        sql = (
            "SELECT t2.idcorresponsa "
            "FROM thcorresponsa t2 "
            "WHERE t2.descrcorresponsa LIKE %s "
            "LIMIT 1"
        )

        for nome in valores:
            corresponsa_id = None
            for padrao in (f"%{nome} %", f"% {nome}%", f"%{nome}%"):
                cur.execute(sql, (padrao,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    corresponsa_id = int(row[0])
                    break
            resultado[nome] = corresponsa_id
            if corresponsa_id is None:
                nomes_nao_encontrados.append(nome)

        return resultado, nomes_nao_encontrados
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def buscar_ids_tipo_evento(valores_coluna: pd.Series) -> tuple[dict[str, Optional[int]], List[str]]:
    valores = []
    vistos = set()

    for valor in valores_coluna.fillna("").astype(str):
        nome = valor.strip()
        if nome and nome not in vistos:
            vistos.add(nome)
            valores.append(nome)

    if not valores:
        return {}, []

    conn = None
    cur = None
    resultado: dict[str, Optional[int]] = {}
    nomes_nao_encontrados: List[str] = []

    try:
        conn, cur = conectar_ao_mysql()
        sql = (
            "SELECT t.iddecorrente "
            "FROM thdecor t "
            "WHERE t.descrdecorrente LIKE %s "
            "LIMIT 1"
        )

        for nome in valores:
            decorrente_id = None
            for padrao in (f"%{nome}%",):
                cur.execute(sql, (padrao,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    decorrente_id = int(row[0])
                    break
            resultado[nome] = decorrente_id
            if decorrente_id is None:
                nomes_nao_encontrados.append(nome)

        return resultado, nomes_nao_encontrados
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def aplicar_nomegrupo_id(df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    if "cliente" not in df.columns:
        return df, []

    mapa, clientes_sem_grupo = buscar_nomegrupo_ids_por_cliente(df["cliente"])
    df = df.copy()
    clientes_normalizados = df["cliente"].fillna("").astype(str).str.strip()
    df["nomegrupo_id"] = pd.to_numeric(clientes_normalizados.map(mapa), errors="coerce").astype("Int64")
    return df, clientes_sem_grupo


def aplicar_ids_por_fullname(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, List[str]]]:
    df = df.copy()
    nao_encontrados: Dict[str, List[str]] = {}

    for coluna in COLUNAS_AUTH_USER_FULLNAME:
        if coluna not in df.columns:
            continue

        mapa, faltantes = buscar_ids_por_fullname(df[coluna])
        valores_normalizados = df[coluna].fillna("").astype(str).str.strip()
        df[coluna] = pd.to_numeric(valores_normalizados.map(mapa), errors="coerce").astype("Int64")

        if faltantes:
            nao_encontrados[coluna] = faltantes

    return df, nao_encontrados


def aplicar_corresponsavel_id(df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    if "corresponsavel" not in df.columns:
        return df, []

    mapa, faltantes = buscar_ids_corresponsavel(df["corresponsavel"])
    df = df.copy()
    valores_normalizados = df["corresponsavel"].fillna("").astype(str).str.strip()
    df["corresponsavel"] = pd.to_numeric(valores_normalizados.map(mapa), errors="coerce").astype("Int64")
    return df, faltantes


def aplicar_tipo_evento_id(df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    if "tipoEvento" not in df.columns:
        return df, []

    mapa, faltantes = buscar_ids_tipo_evento(df["tipoEvento"])
    df = df.copy()
    valores_normalizados = df["tipoEvento"].fillna("").astype(str).str.strip()
    df["tipoEvento"] = pd.to_numeric(valores_normalizados.map(mapa), errors="coerce").astype("Int64")
    return df, faltantes


def inserir_em_lotes_sem_tratamento(
    df: pd.DataFrame,
    colunas: List[str],
    lote: int = 500,
    progress_cb=None,
) -> Tuple[int, Optional[str]]:
    conn = None
    cur = None
    try:
        registros = montar_registros_crus(df, colunas)
        if not registros:
            return 0, None

        conn, cur = conectar_ao_mysql()
        sql_cols = ", ".join(colunas)
        placeholders = ", ".join(["%s"] * len(colunas))
        sql = f"INSERT INTO thproc ({sql_cols}) VALUES ({placeholders})"

        total = 0
        for inicio in range(0, len(registros), lote):
            chunk = registros[inicio:inicio + lote]
            cur.executemany(sql, chunk)
            conn.commit()
            total += cur.rowcount or 0
            if progress_cb:
                progress_cb(min(inicio + len(chunk), len(registros)), len(registros))

        return total, None
    except Exception as err:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return 0, str(err)
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


class MigracoesSemTratamentoApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Executavel - sem tratamento")
        self.geometry("1100x680")
        self.state = AppState()
        self.carteiras_map: Dict[str, Tuple[int, str]] = {}
        self.create_widgets()
        self.after(200, self._load_carteiras_async)

    def create_widgets(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Planilha:").grid(row=0, column=0, sticky="w")
        self.ent_path = ttk.Entry(top, width=90)
        self.ent_path.grid(row=0, column=1, padx=5)
        ttk.Button(top, text="Selecionar...", command=self.on_select_file).grid(row=0, column=2)

        ttk.Label(top, text="Aba (sheet):").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_sheet = ttk.Combobox(top, values=[], state="readonly", width=30)
        self.cmb_sheet.grid(row=1, column=1, sticky="w", pady=(8, 0))

        ttk.Label(top, text="Carteira:").grid(row=1, column=2, sticky="e", pady=(8, 0))
        self.cmb_carteira = ttk.Combobox(top, values=[], state="readonly", width=40)
        self.cmb_carteira.grid(row=1, column=3, sticky="w", padx=5, pady=(8, 0))
        self.cmb_carteira.bind("<<ComboboxSelected>>", self.on_carteira_selected)

        btns = ttk.Frame(top)
        btns.grid(row=2, column=0, columnspan=4, sticky="w", pady=10)
        ttk.Button(btns, text="Testar Conexao", command=self.on_test_conn).pack(side="left")
        ttk.Button(btns, text="Pre-visualizar", command=self.on_preview).pack(side="left", padx=6)
        ttk.Button(btns, text="Enviar ao Banco", command=self.on_send).pack(side="left")
        ttk.Button(btns, text="Gerar Planilha do Preview", command=self.on_export_preview).pack(side="left", padx=6)
        ttk.Button(btns, text="Recarregar Carteiras", command=self._load_carteiras_async).pack(side="left", padx=6)

        self.tree = ttk.Treeview(self, columns=("info",), show="headings")
        self.tree.pack(fill="both", expand=True, padx=10, pady=10)

        feedback = ttk.LabelFrame(self, text="Criticas e Avisos", padding=10)
        feedback.pack(fill="both", padx=10, pady=(0, 10))
        self.txt_feedback = tk.Text(feedback, height=10, wrap="word")
        self.txt_feedback.pack(fill="both", expand=True)
        self.txt_feedback.configure(state="disabled")

        status = ttk.Frame(self, padding=(10, 0))
        status.pack(fill="x")
        self.lbl_status = ttk.Label(status, text="Pronto.")
        self.lbl_status.pack(side="left")
        self.pb = ttk.Progressbar(status, mode="determinate", length=300)
        self.pb.pack(side="right")

    def on_select_file(self):
        path = filedialog.askopenfilename(
            title="Selecione a planilha Excel",
            filetypes=[("Arquivos Excel", "*.xlsx *.xls")],
        )
        if not path:
            return

        self.state = AppState(path=path)
        self.ent_path.delete(0, tk.END)
        self.ent_path.insert(0, path)

        try:
            xl = pd.ExcelFile(path)
            sheets = xl.sheet_names
            self.cmb_sheet["values"] = sheets
            if sheets:
                self.cmb_sheet.set(sheets[0])
                self.state.sheet_name = sheets[0]
            self._set_feedback([], [])
            self.lbl_status["text"] = "Planilha carregada."
        except Exception as err:
            messagebox.showerror("Erro", f"Nao foi possivel ler as abas:\n{err}")

    def _formatar_item_combo(self, item_id: int, nome: str) -> str:
        return f"{nome.strip()} [id {item_id}]"

    def _load_carteiras_async(self):
        self.lbl_status["text"] = "Carregando carteiras..."

        def worker():
            ok, carteiras, msg = listar_carteiras()
            self.after(0, lambda: self._finish_load_carteiras(ok, carteiras, msg))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_load_carteiras(self, ok: bool, carteiras: List[Tuple[int, str]], msg: str):
        if not ok:
            self.lbl_status["text"] = "Falha ao carregar carteiras."
            messagebox.showerror("Erro", msg)
            return

        self.carteiras_map = {
            self._formatar_item_combo(item_id, nome): (item_id, nome.strip())
            for item_id, nome in carteiras
        }
        valores = list(self.carteiras_map.keys())
        self.cmb_carteira["values"] = valores

        atual = self.cmb_carteira.get()
        if atual in self.carteiras_map:
            self.on_carteira_selected()
        else:
            self.cmb_carteira.set("")
            self.state.carteira_id = None
            self.state.carteira_nome = ""

        self.lbl_status["text"] = f"{len(valores)} carteiras carregadas."

    def on_carteira_selected(self, _event=None):
        selecionada = self.cmb_carteira.get().strip()
        carteira = self.carteiras_map.get(selecionada)
        if carteira is None:
            self.state.carteira_id = None
            self.state.carteira_nome = ""
            return

        self.state.carteira_id = carteira[0]
        self.state.carteira_nome = carteira[1]

    def on_test_conn(self):
        self.lbl_status["text"] = "Testando conexao..."

        def worker():
            try:
                conn, cur = conectar_ao_mysql()
                cur.execute("SELECT 1")
                cur.close()
                conn.close()
                self.after(0, lambda: self._finish_test_conn(True, "Conexao com MySQL OK!"))
            except Exception as err:
                msg = str(err)
                self.after(0, lambda msg=msg: self._finish_test_conn(False, msg))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_test_conn(self, ok: bool, msg: str):
        if ok:
            messagebox.showinfo("Conexao", msg)
            self.lbl_status["text"] = "Pronto."
        else:
            messagebox.showerror("Erro MySQL", msg)
            self.lbl_status["text"] = "Falha na conexao."

    def _set_feedback(self, criticas: List[str], avisos: List[str]):
        self.state.criticas = criticas
        self.state.avisos = avisos

        linhas = []
        if criticas:
            linhas.append("CRITICAS:")
            linhas.extend(f"- {item}" for item in criticas)
        if avisos:
            if linhas:
                linhas.append("")
            linhas.append("AVISOS:")
            linhas.extend(f"- {item}" for item in avisos)
        if not linhas:
            linhas.append("Nenhuma critica ou aviso gerado no preview.")

        self.txt_feedback.configure(state="normal")
        self.txt_feedback.delete("1.0", tk.END)
        self.txt_feedback.insert("1.0", "\n".join(linhas))
        self.txt_feedback.configure(state="disabled")

    def on_preview(self):
        if not self.state.path:
            messagebox.showwarning("Atencao", "Selecione uma planilha primeiro.")
            return

        sheet = self.cmb_sheet.get() or 0
        try:
            self.lbl_status["text"] = "Lendo planilha..."
            df = pd.read_excel(self.state.path, sheet_name=sheet)
            df = normalizar_colunas_planilha(df)
            criticas = []
            avisos = []

            if self.state.carteira_id is None or not self.state.carteira_nome:
                criticas.append("Selecione uma carteira para preencher os campos 'carteira' e 'codlote'.")

            if criticas:
                self._set_feedback(criticas, avisos)
                self.lbl_status["text"] = "Preview bloqueado."
                return

            df = aplicar_carteira_e_codlote(df, self.state.carteira_id, self.state.carteira_nome)
            df, clientes_sem_grupo = aplicar_nomegrupo_id(df)
            df, campos_sem_id = aplicar_ids_por_fullname(df)
            df, corresponsaveis_sem_id = aplicar_corresponsavel_id(df)
            df, tipos_evento_sem_id = aplicar_tipo_evento_id(df)
            df = preparar_dataframe_para_insert(df)

            if "cliente" not in df.columns:
                criticas.append("Coluna 'cliente' nao encontrada. 'nomegrupo_id' nao foi gerado.")

            linhas_cnj_ignoradas: List[int] = []
            if "cnj" not in df.columns:
                criticas.append("Coluna obrigatoria 'cnj' nao encontrada na planilha.")
            else:
                linhas_admin_sem_cnj = linhas_administrativas_sem_cnj(df)
                if linhas_admin_sem_cnj:
                    exibicao_linhas = ", ".join(map(str, linhas_admin_sem_cnj[:10]))
                    if len(linhas_admin_sem_cnj) > 10:
                        exibicao_linhas += " ..."
                    criticas.append(
                        "Processos com natureza 'Administrativa' devem ter o campo 'cnj' preenchido. "
                        f"Linhas com erro: {exibicao_linhas}."
                    )
                df, linhas_cnj_ignoradas = filtrar_linhas_sem_cnj(df)
                if linhas_cnj_ignoradas:
                    exibicao_linhas = ", ".join(map(str, linhas_cnj_ignoradas[:10]))
                    if len(linhas_cnj_ignoradas) > 10:
                        exibicao_linhas += " ..."
                    avisos.append(
                        "Linhas ignoradas por 'cnj' vazio: "
                        f"{exibicao_linhas}"
                    )
                if df.empty:
                    criticas.append("Nenhuma linha valida restou para envio apos ignorar os registros com 'cnj' vazio.")

            if "tipoPoloCliente" not in df.columns:
                criticas.append("Coluna obrigatoria 'Tipo Cliente' nao encontrada na planilha.")
            else:
                df, ok_tipo_polo_cliente, linhas_invalidas_tipo_cliente, valores_invalidos_tipo_cliente = validar_tipo_polo_cliente(df)
                if not ok_tipo_polo_cliente:
                    exibicao_linhas = ", ".join(map(str, linhas_invalidas_tipo_cliente[:10]))
                    if len(linhas_invalidas_tipo_cliente) > 10:
                        exibicao_linhas += " ..."
                    criticas.append(
                        "A coluna 'Tipo Cliente' deve conter apenas 'Ativo' ou 'Passivo'. "
                        f"Linhas com erro: {exibicao_linhas}. "
                        f"Valores encontrados: {', '.join(valores_invalidos_tipo_cliente[:10])}"
                    )

            insert_columns = list(COLUNAS_INSERIVEIS)

            colunas_auth_user_ausentes = [
                coluna for coluna in COLUNAS_AUTH_USER_FULLNAME if coluna not in df.columns
            ]
            if colunas_auth_user_ausentes:
                avisos.append(
                    "Colunas ausentes para consulta em auth_user.fullname: "
                    + ", ".join(colunas_auth_user_ausentes)
                )
            if "corresponsavel" not in df.columns:
                avisos.append("Coluna ausente para consulta em thcorresponsa.descrcorresponsa: corresponsavel")
            if "tipoEvento" not in df.columns:
                avisos.append("Coluna ausente para consulta em thdecor.descrdecorrente: tipoEvento")

            self.state.df = df
            self.state.sheet_name = str(sheet)
            self.state.insert_columns = insert_columns
            self.state.clientes_sem_grupo = clientes_sem_grupo
            self._render_preview(df)

            if clientes_sem_grupo:
                exibicao = ", ".join(clientes_sem_grupo[:10])
                if len(clientes_sem_grupo) > 10:
                    exibicao += " ..."
                criticas.append(
                    "Grupo nao encontrado para os clientes: "
                    f"{exibicao}"
                )

            for coluna, faltantes in campos_sem_id.items():
                exibicao = ", ".join(faltantes[:10])
                if len(faltantes) > 10:
                    exibicao += " ..."
                avisos.append(
                    f"{coluna} sem ID no auth_user.fullname: "
                    f"{exibicao}"
                )
            if corresponsaveis_sem_id:
                exibicao = ", ".join(corresponsaveis_sem_id[:10])
                if len(corresponsaveis_sem_id) > 10:
                    exibicao += " ..."
                avisos.append(
                    "corresponsavel sem ID em thcorresponsa.descrcorresponsa: "
                    f"{exibicao}"
                )
            if tipos_evento_sem_id:
                exibicao = ", ".join(tipos_evento_sem_id[:10])
                if len(tipos_evento_sem_id) > 10:
                    exibicao += " ..."
                avisos.append(
                    "tipoEvento sem ID em thdecor.descrdecorrente: "
                    f"{exibicao}"
                )

            self._set_feedback(criticas, avisos)

            if insert_columns:
                self.lbl_status["text"] = (
                    f"Pre-visualizacao OK - {len(df)} linhas. "
                    f"{len(insert_columns)} colunas prontas para envio."
                )
                if linhas_cnj_ignoradas:
                    self.lbl_status["text"] += f" {len(linhas_cnj_ignoradas)} linha(s) ignorada(s) por CNJ vazio."
                if clientes_sem_grupo:
                    self.lbl_status["text"] += f" {len(clientes_sem_grupo)} cliente(s) sem grupo."
                if campos_sem_id:
                    total_sem_id = sum(len(faltantes) for faltantes in campos_sem_id.values())
                    self.lbl_status["text"] += f" {total_sem_id} valor(es) sem ID em auth_user.fullname."
                if corresponsaveis_sem_id:
                    self.lbl_status["text"] += (
                        f" {len(corresponsaveis_sem_id)} valor(es) sem ID em thcorresponsa."
                    )
                if tipos_evento_sem_id:
                    self.lbl_status["text"] += (
                        f" {len(tipos_evento_sem_id)} valor(es) sem ID em thdecor."
                    )
            else:
                self.lbl_status["text"] = "Pre-visualizacao OK, mas nenhuma coluna corresponde a thproc."
        except Exception as err:
            self._set_feedback([f"Falha ao pre-visualizar: {err}"], [])
            messagebox.showerror("Erro", f"Falha ao pre-visualizar:\n{err}")

    def on_export_preview(self):
        if self.state.df is None:
            messagebox.showwarning("Atencao", "Faca a pre-visualizacao antes de gerar a planilha.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_saida = filedialog.asksaveasfilename(
            title="Salvar planilha do preview",
            defaultextension=".xlsx",
            filetypes=[("Arquivos Excel", "*.xlsx")],
            initialfile=f"preview_{timestamp}.xlsx",
        )

        if not arquivo_saida:
            return

        try:
            self.state.df.to_excel(arquivo_saida, index=False, sheet_name="Preview")
            self.lbl_status["text"] = "Planilha do preview gerada."
            messagebox.showinfo("Sucesso", f"Planilha salva em:\n{arquivo_saida}")
        except Exception as err:
            messagebox.showerror("Erro", f"Falha ao gerar a planilha do preview:\n{err}")

    def _render_preview(self, df: pd.DataFrame, max_rows: int = 200):
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
        self.tree.delete(*self.tree.get_children())

        show_df = df.copy().head(max_rows)
        cols = [str(col) for col in show_df.columns]
        self.tree["columns"] = cols

        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, stretch=True)

        for _, row in show_df.iterrows():
            values = ["" if pd.isna(row[col]) else str(row[col]) for col in show_df.columns]
            self.tree.insert("", "end", values=values)

    def on_send(self):
        if self.state.df is None:
            messagebox.showwarning("Atencao", "Faca a pre-visualizacao antes de enviar.")
            return

        if not self.state.insert_columns:
            messagebox.showwarning(
                "Atencao",
                "A planilha nao possui colunas com nomes compativeis com a tabela thproc.",
            )
            return

        df_envio, colunas_envio = preparar_campos_submit(self.state.df, self.state.insert_columns)
        df_envio = df_envio[colunas_envio].copy()
        if "cnj" not in df_envio.columns:
            messagebox.showerror("Envio bloqueado", "Coluna obrigatoria 'cnj' nao encontrada.")
            self.lbl_status["text"] = "Envio bloqueado por erro no campo cnj."
            return

        linhas_admin_sem_cnj = linhas_administrativas_sem_cnj(df_envio)
        if linhas_admin_sem_cnj:
            exibicao_linhas = ", ".join(map(str, linhas_admin_sem_cnj[:10]))
            if len(linhas_admin_sem_cnj) > 10:
                exibicao_linhas += " ..."
            messagebox.showerror(
                "Envio bloqueado",
                "Processos com natureza 'Administrativa' devem ter o campo 'cnj' preenchido. "
                f"Linhas com erro: {exibicao_linhas}.",
            )
            self.lbl_status["text"] = "Envio bloqueado por CNJ vazio em natureza Administrativa."
            return

        df_envio, linhas_cnj_ignoradas = filtrar_linhas_sem_cnj(df_envio)
        if df_envio.empty:
            messagebox.showwarning(
                "Envio bloqueado",
                "Nenhuma linha valida restou para envio apos ignorar os registros com 'cnj' vazio.",
            )
            self.lbl_status["text"] = "Nenhuma linha valida para envio."
            return

        if "tipoPoloCliente" not in df_envio.columns:
            messagebox.showerror("Envio bloqueado", "Coluna obrigatoria 'Tipo Cliente' nao encontrada.")
            self.lbl_status["text"] = "Envio bloqueado por erro na coluna Tipo Cliente."
            return

        df_envio, ok_tipo_polo_cliente, linhas_invalidas_tipo_cliente, valores_invalidos_tipo_cliente = validar_tipo_polo_cliente(df_envio)
        if not ok_tipo_polo_cliente:
            exibicao_linhas = ", ".join(map(str, linhas_invalidas_tipo_cliente[:10]))
            if len(linhas_invalidas_tipo_cliente) > 10:
                exibicao_linhas += " ..."
            messagebox.showerror(
                "Envio bloqueado",
                "A coluna 'Tipo Cliente' deve conter apenas 'Ativo' ou 'Passivo'. "
                f"Linhas com erro: {exibicao_linhas}. "
                f"Valores encontrados: {', '.join(valores_invalidos_tipo_cliente[:10])}",
            )
            self.lbl_status["text"] = "Envio bloqueado por erro na coluna Tipo Cliente."
            return

        self.pb["value"] = 0
        self.pb["maximum"] = len(df_envio)
        self.lbl_status["text"] = "Enviando ao banco..."
        if linhas_cnj_ignoradas:
            self.lbl_status["text"] += f" Ignorando {len(linhas_cnj_ignoradas)} linha(s) sem CNJ."

        def progress_cb(done: int, total: int):
            self.after(0, lambda: self._update_progress(done, total))

        def worker():
            total, error_msg = inserir_em_lotes_sem_tratamento(
                df=df_envio,
                colunas=colunas_envio,
                lote=500,
                progress_cb=progress_cb,
            )
            self.after(0, lambda: self._finish_send(total, error_msg))

        threading.Thread(target=worker, daemon=True).start()

    def _update_progress(self, done: int, total: int):
        self.pb["value"] = done
        self.lbl_status["text"] = f"Inserindo... {done}/{total}"
        self.update_idletasks()

    def _finish_send(self, total: int, error_msg: Optional[str]):
        if error_msg:
            self.lbl_status["text"] = "Falha no envio."
            messagebox.showerror("Erro MySQL", error_msg)
            return

        self.pb["value"] = self.pb["maximum"]
        self.lbl_status["text"] = f"Concluido. Inseridos {total} registros."
        messagebox.showinfo("Finalizado", f"Inseridos {total} registros.")


if __name__ == "__main__":
    app = MigracoesSemTratamentoApp()
    app.mainloop()
