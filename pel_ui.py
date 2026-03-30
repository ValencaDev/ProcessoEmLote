from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from pel_config import (
    COMPANY_PRESETS,
    LAST_ENV_PATH,
    OPTIONAL_INPUT_DEFAULTS,
    RENAME_MAP,
    colunas_thproc,
    colunas_thproc_regular,
    obter_config_banco,
)
from pel_db import (
    conectar_ao_mysql,
    inserir_em_lotes,
    listar_carteiras,
    listar_grupos_por_carteira,
)
from pel_importacao import (
    aplicar_presets,
    formatar_datas_e_numeros,
    montar_registros,
    normalizar_vazios_para_null,
    preparar_dataframe,
    usa_fluxo_regular,
    validar_campo_cnj,
    validar_campo_natureza,
    validar_colunas_para_insercao,
)

@dataclass
class AppState:
    path: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    empresa: str = ''
    sheet_name: Optional[str] = None
    missing_columns: List[str] = field(default_factory=list)
    carteira_id: Optional[int] = None
    carteira_nome: str = ''
    grupo_id: Optional[int] = None
    grupo_nome: str = ''

class MigracoesApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Executável – Importador thproc')
        self.geometry('1100x680')
        self.state = AppState()
        self.carteiras_map: Dict[str, int] = {}
        self.grupos_map: Dict[str, int] = {}
        self.create_widgets()

    # ---------- UI Builders ----------
    def create_widgets(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill='x')

        # Arquivo
        ttk.Label(top, text='Planilha:').grid(row=0, column=0, sticky='w')
        self.ent_path = ttk.Entry(top, width=90)
        self.ent_path.grid(row=0, column=1, padx=5)
        ttk.Button(top, text='Selecionar...', command=self.on_select_file).grid(row=0, column=2)

        # Sheet/Empresa
        ttk.Label(top, text='Aba (sheet):').grid(row=1, column=0, sticky='w', pady=(8, 0))
        self.cmb_sheet = ttk.Combobox(top, values=[], state='readonly', width=30)
        self.cmb_sheet.grid(row=1, column=1, sticky='w', pady=(8, 0))

        ttk.Label(top, text='Empresa:').grid(row=1, column=2, sticky='e', pady=(8, 0))
        self.cmb_empresa = ttk.Combobox(
            top,
            values=list(COMPANY_PRESETS.keys()),
            state='readonly',
            width=40
        )
        self.cmb_empresa.grid(row=1, column=3, sticky='w', padx=5, pady=(8, 0))

        # Botões
        btns = ttk.Frame(top)
        btns.grid(row=2, column=0, columnspan=4, sticky='w', pady=10)
        ttk.Button(btns, text='Testar Conexão', command=self.on_test_conn).pack(side='left')
        ttk.Button(btns, text='Pré-visualizar', command=self.on_preview).pack(side='left', padx=6)
        ttk.Button(btns, text='Enviar ao Banco', command=self.on_send).pack(side='left')

        # Árvore de preview
        self.tree = ttk.Treeview(self, columns=('info',), show='headings')
        self.tree.pack(fill='both', expand=True, padx=10, pady=10)

        # Barra de status + progresso
        status = ttk.Frame(self, padding=(10, 0))
        status.pack(fill='x')
        self.lbl_status = ttk.Label(status, text='Pronto.')
        self.lbl_status.pack(side='left')
        self.pb = ttk.Progressbar(status, mode='determinate', length=300)
        self.pb.pack(side='right')

    # ---------- Handlers ----------
    def on_select_file(self):
        path = filedialog.askopenfilename(
            title='Selecione a planilha Excel',
            filetypes=[('Arquivos Excel', '*.xlsx *.xls')]
        )
        if not path:
            return
        self.state.path = path
        self.state.df = None
        self.state.missing_columns = []
        self.ent_path.delete(0, tk.END)
        self.ent_path.insert(0, path)
        try:
            xl = pd.ExcelFile(path)
            sheets = xl.sheet_names
            self.cmb_sheet['values'] = sheets
            if sheets:
                self.cmb_sheet.set(sheets[0])
                self.state.sheet_name = sheets[0]
        except Exception as e:
            messagebox.showerror('Erro', f'Não foi possível ler as abas:\n{e}')

    def _formatar_item_combo(self, item_id: int, nome: str) -> str:
        return f'{nome.strip()} [id {item_id}]'

    def _load_carteiras_async(self):
        self.lbl_status['text'] = 'Carregando carteiras...'

        def worker():
            ok, carteiras, msg = listar_carteiras()
            self.after(0, lambda: self._finish_load_carteiras(ok, carteiras, msg))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_load_carteiras(self, ok: bool, carteiras: List[Tuple[int, str]], msg: str):
        if not ok:
            self.lbl_status['text'] = 'Falha ao carregar carteiras.'
            messagebox.showerror('Erro', msg)
            return

        self.carteiras_map = {
            self._formatar_item_combo(item_id, nome): item_id
            for item_id, nome in carteiras
        }
        valores = list(self.carteiras_map.keys())
        self.cmb_carteira['values'] = valores

        atual = self.cmb_carteira.get()
        if atual in self.carteiras_map:
            self.on_carteira_selected()
        else:
            self.cmb_carteira.set('')
            self.cmb_grupo.set('')
            self.cmb_grupo['values'] = []
            self.grupos_map = {}
            self.state.carteira_id = None
            self.state.carteira_nome = ''
            self.state.grupo_id = None
            self.state.grupo_nome = ''

        self.lbl_status['text'] = f'{len(valores)} carteiras carregadas.'

    def on_carteira_selected(self, _event=None):
        selecionada = self.cmb_carteira.get().strip()
        carteira_id = self.carteiras_map.get(selecionada)
        self.state.carteira_id = carteira_id
        self.state.carteira_nome = selecionada
        self.state.grupo_id = None
        self.state.grupo_nome = ''
        self.cmb_grupo.set('')
        self.cmb_grupo['values'] = []
        self.grupos_map = {}

        if carteira_id is not None:
            self._load_grupos_async()

    def _load_grupos_async(self):
        carteira_id = self.carteiras_map.get(self.cmb_carteira.get().strip())
        if carteira_id is None:
            return

        self.lbl_status['text'] = 'Carregando grupos...'

        def worker():
            ok, grupos, msg = listar_grupos_por_carteira(carteira_id)
            self.after(0, lambda: self._finish_load_grupos(carteira_id, ok, grupos, msg))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_load_grupos(
        self,
        carteira_id: int,
        ok: bool,
        grupos: List[Tuple[int, str]],
        msg: str
    ):
        if carteira_id != self.carteiras_map.get(self.cmb_carteira.get().strip()):
            return

        if not ok:
            self.lbl_status['text'] = 'Falha ao carregar grupos.'
            messagebox.showerror('Erro', msg)
            return

        self.grupos_map = {
            self._formatar_item_combo(item_id, nome): item_id
            for item_id, nome in grupos
        }
        valores = list(self.grupos_map.keys())
        self.cmb_grupo['values'] = valores

        if valores:
            self.cmb_grupo.set(valores[0])
            self.state.grupo_nome = valores[0]
            self.state.grupo_id = self.grupos_map[valores[0]]
        else:
            self.cmb_grupo.set('')
            self.state.grupo_nome = ''
            self.state.grupo_id = None

        self.lbl_status['text'] = f'{len(valores)} grupos carregados.'

    def open_alt_window(self):
        if getattr(self, 'alt_window', None) and self.alt_window.winfo_exists():
            self.alt_window.lift()
            self.alt_window.focus_force()
            return

        self.alt_state = AppState()
        self.alt_carteiras_map: Dict[str, int] = {}
        self.alt_grupos_map: Dict[str, int] = {}

        win = tk.Toplevel(self)
        win.title('Janela Alternativa - Envio de Planilha')
        win.geometry('980x620')
        win.transient(self)
        self.alt_window = win

        top = ttk.Frame(win, padding=10)
        top.pack(fill='x')

        ttk.Label(top, text='Planilha:').grid(row=0, column=0, sticky='w')
        self.alt_ent_path = ttk.Entry(top, width=78)
        self.alt_ent_path.grid(row=0, column=1, padx=5, sticky='we')
        ttk.Button(top, text='Selecionar...', command=self.alt_on_select_file).grid(row=0, column=2)

        ttk.Label(top, text='Aba (sheet):').grid(row=1, column=0, sticky='w', pady=(8, 0))
        self.alt_cmb_sheet = ttk.Combobox(top, values=[], state='readonly', width=28)
        self.alt_cmb_sheet.grid(row=1, column=1, sticky='w', pady=(8, 0))

        ttk.Label(top, text='Carteira:').grid(row=2, column=0, sticky='w', pady=(8, 0))
        self.alt_cmb_carteira = ttk.Combobox(top, values=[], state='readonly', width=48)
        self.alt_cmb_carteira.grid(row=2, column=1, sticky='w', pady=(8, 0))
        self.alt_cmb_carteira.bind('<<ComboboxSelected>>', self.alt_on_carteira_selected)

        ttk.Label(top, text='Grupo:').grid(row=3, column=0, sticky='w', pady=(8, 0))
        self.alt_cmb_grupo = ttk.Combobox(top, values=[], state='readonly', width=48)
        self.alt_cmb_grupo.grid(row=3, column=1, sticky='w', pady=(8, 0))

        top.columnconfigure(1, weight=1)

        btns = ttk.Frame(win, padding=(10, 0))
        btns.pack(fill='x')
        ttk.Button(btns, text='Recarregar Carteiras', command=self.alt_load_carteiras_async).pack(side='left')
        ttk.Button(btns, text='Recarregar Grupos', command=self.alt_load_grupos_async).pack(side='left', padx=6)
        ttk.Button(btns, text='Pré-visualizar', command=self.alt_on_preview).pack(side='left')
        ttk.Button(btns, text='Enviar Planilha', command=self.alt_on_send).pack(side='left', padx=6)

        self.alt_tree = ttk.Treeview(win, columns=('info',), show='headings')
        self.alt_tree.pack(fill='both', expand=True, padx=10, pady=10)

        status = ttk.Frame(win, padding=(10, 0, 10, 10))
        status.pack(fill='x')
        self.alt_lbl_status = ttk.Label(status, text='Pronto.')
        self.alt_lbl_status.pack(side='left')
        self.alt_pb = ttk.Progressbar(status, mode='determinate', length=280)
        self.alt_pb.pack(side='right')

        self.alt_load_carteiras_async()

    def alt_on_select_file(self):
        path = filedialog.askopenfilename(
            title='Selecione a planilha Excel',
            filetypes=[('Arquivos Excel', '*.xlsx *.xls')]
        )
        if not path:
            return

        self.alt_state.path = path
        self.alt_state.df = None
        self.alt_ent_path.delete(0, tk.END)
        self.alt_ent_path.insert(0, path)

        try:
            xl = pd.ExcelFile(path)
            sheets = xl.sheet_names
            self.alt_cmb_sheet['values'] = sheets
            if sheets:
                self.alt_cmb_sheet.set(sheets[0])
                self.alt_state.sheet_name = sheets[0]
        except Exception as e:
            messagebox.showerror('Erro', f'Não foi possível ler as abas:\n{e}', parent=self.alt_window)

    def alt_load_carteiras_async(self):
        self.alt_lbl_status['text'] = 'Carregando carteiras...'

        def worker():
            ok, carteiras, msg = listar_carteiras()
            self.after(0, lambda: self.alt_finish_load_carteiras(ok, carteiras, msg))

        threading.Thread(target=worker, daemon=True).start()

    def alt_finish_load_carteiras(self, ok: bool, carteiras: List[Tuple[int, str]], msg: str):
        if not getattr(self, 'alt_window', None) or not self.alt_window.winfo_exists():
            return
        if not ok:
            self.alt_lbl_status['text'] = 'Falha ao carregar carteiras.'
            messagebox.showerror('Erro', msg, parent=self.alt_window)
            return

        self.alt_carteiras_map = {
            self._formatar_item_combo(item_id, nome): item_id
            for item_id, nome in carteiras
        }
        self.alt_cmb_carteira['values'] = list(self.alt_carteiras_map.keys())
        self.alt_cmb_carteira.set('')
        self.alt_cmb_grupo.set('')
        self.alt_cmb_grupo['values'] = []
        self.alt_grupos_map = {}
        self.alt_lbl_status['text'] = f'{len(self.alt_carteiras_map)} carteiras carregadas.'

    def alt_on_carteira_selected(self, _event=None):
        self.alt_state.carteira_nome = self.alt_cmb_carteira.get().strip()
        self.alt_state.carteira_id = self.alt_carteiras_map.get(self.alt_state.carteira_nome)
        self.alt_state.grupo_id = None
        self.alt_state.grupo_nome = ''
        self.alt_cmb_grupo.set('')
        self.alt_cmb_grupo['values'] = []
        self.alt_grupos_map = {}
        if self.alt_state.carteira_id is not None:
            self.alt_load_grupos_async()

    def alt_load_grupos_async(self):
        carteira_id = self.alt_carteiras_map.get(self.alt_cmb_carteira.get().strip())
        if carteira_id is None:
            return

        self.alt_lbl_status['text'] = 'Carregando grupos...'

        def worker():
            ok, grupos, msg = listar_grupos_por_carteira(carteira_id)
            self.after(0, lambda: self.alt_finish_load_grupos(carteira_id, ok, grupos, msg))

        threading.Thread(target=worker, daemon=True).start()

    def alt_finish_load_grupos(
        self,
        carteira_id: int,
        ok: bool,
        grupos: List[Tuple[int, str]],
        msg: str
    ):
        if not getattr(self, 'alt_window', None) or not self.alt_window.winfo_exists():
            return
        if carteira_id != self.alt_carteiras_map.get(self.alt_cmb_carteira.get().strip()):
            return
        if not ok:
            self.alt_lbl_status['text'] = 'Falha ao carregar grupos.'
            messagebox.showerror('Erro', msg, parent=self.alt_window)
            return

        self.alt_grupos_map = {
            self._formatar_item_combo(item_id, nome): item_id
            for item_id, nome in grupos
        }
        valores = list(self.alt_grupos_map.keys())
        self.alt_cmb_grupo['values'] = valores
        if valores:
            self.alt_cmb_grupo.set(valores[0])
            self.alt_state.grupo_nome = valores[0]
            self.alt_state.grupo_id = self.alt_grupos_map[valores[0]]
        self.alt_lbl_status['text'] = f'{len(valores)} grupos carregados.'

    def alt_on_preview(self):
        if not self.alt_state.path:
            messagebox.showwarning('Atenção', 'Selecione uma planilha primeiro.', parent=self.alt_window)
            return

        sheet = self.alt_cmb_sheet.get() or 0
        carteira_label = self.alt_cmb_carteira.get().strip()
        grupo_label = self.alt_cmb_grupo.get().strip()
        carteira_id = self.alt_carteiras_map.get(carteira_label)
        grupo_id = self.alt_grupos_map.get(grupo_label)

        if carteira_id is None:
            messagebox.showwarning('Atenção', 'Selecione uma carteira.', parent=self.alt_window)
            return
        if grupo_id is None:
            messagebox.showwarning('Atenção', 'Selecione um grupo.', parent=self.alt_window)
            return

        try:
            self.alt_lbl_status['text'] = 'Lendo planilha...'
            df = pd.read_excel(self.alt_state.path, sheet_name=sheet)
            df = df.rename(columns=RENAME_MAP)
            df = normalizar_vazios_para_null(df)
            df['carteira'] = str(carteira_id)
            df['nomegrupo_id'] = int(grupo_id)
            for col in colunas_thproc:
                if col not in df.columns:
                    df[col] = None

            self.alt_state.df = df
            self.alt_state.sheet_name = str(sheet)
            self.alt_state.carteira_id = carteira_id
            self.alt_state.carteira_nome = carteira_label
            self.alt_state.grupo_id = grupo_id
            self.alt_state.grupo_nome = grupo_label
            self._render_preview_to_tree(self.alt_tree, df)
            self.alt_lbl_status['text'] = f'Pré-visualização OK - {len(df)} linhas.'
        except Exception as e:
            messagebox.showerror('Erro', f'Falha ao pré-visualizar:\n{e}', parent=self.alt_window)

    def alt_on_send(self):
        if self.alt_state.df is None or self.alt_state.df.empty:
            messagebox.showwarning('Atenção', 'Faça a pré-visualização antes de enviar.', parent=self.alt_window)
            return

        df = self.alt_state.df.copy()
        registros = montar_registros(df)

        self.alt_pb['value'] = 0
        self.alt_pb['maximum'] = len(registros)
        self.alt_lbl_status['text'] = 'Enviando ao banco...'

        def progress_cb(done, total):
            self.after(0, lambda: self._alt_update_progress(done, total))

        def worker():
            total, cnjs_duplicados, error_msg = inserir_em_lotes(
                registros,
                lote=500,
                progress_cb=progress_cb
            )
            self.after(0, lambda: self._alt_finish_send(df, total, cnjs_duplicados, error_msg))

        threading.Thread(target=worker, daemon=True).start()

    def _render_preview_to_tree(self, tree: ttk.Treeview, df: pd.DataFrame, max_rows: int = 200):
        for col in tree['columns']:
            tree.heading(col, text='')
        tree.delete(*tree.get_children())

        show_df = df.copy().head(max_rows)
        cols = list(show_df.columns)
        tree['columns'] = cols
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=120, stretch=True)
        for _, row in show_df.iterrows():
            values = [str(row[c]) if pd.notna(row[c]) else '' for c in cols]
            tree.insert('', 'end', values=values)

    def _alt_update_progress(self, done: int, total: int):
        self.alt_pb['value'] = done
        self.alt_lbl_status['text'] = f'Inserindo... {done}/{total}'
        self.alt_window.update_idletasks()

    def _alt_finish_send(
        self,
        df: pd.DataFrame,
        total: int,
        cnjs_duplicados: List[str],
        error_msg: Optional[str]
    ):
        if error_msg:
            self.alt_lbl_status['text'] = 'Falha no envio.'
            messagebox.showerror('Erro MySQL', error_msg, parent=self.alt_window)
            return

        if cnjs_duplicados:
            messagebox.showinfo(
                'Duplicados Detectados',
                f'Encontrados {len(cnjs_duplicados)} processos duplicados.\n'
                f'Serão inseridos apenas {self.alt_pb["maximum"] - len(cnjs_duplicados)} processos novos.',
                parent=self.alt_window
            )

        self.alt_pb['value'] = self.alt_pb['maximum']
        self.alt_lbl_status['text'] = f'Concluído. Inseridos {total} registros.'
        messagebox.showinfo('Finalizado', f'Inseridos {total} registros.', parent=self.alt_window)

    def on_test_conn(self):
        cfg = obter_config_banco()
        messagebox.showinfo(
            "DEBUG",
            f"Host={cfg['host']}\nUser={cfg['user']}\nDB={cfg['database']}\n"
            f"Timeout={cfg.get('timeout', 15)}s\n.env usado: {LAST_ENV_PATH or 'NÃO ENCONTRADO'}"
        )

        self.lbl_status['text'] = 'Testando conexão...'
        self.update_idletasks()

        def worker():
            try:
                conn, cur = conectar_ao_mysql()
                self.after(0, lambda: self._finish_test_conn(conn, cur, None))
            except Exception as e:
                self.after(0, lambda: self._finish_test_conn(None, None, str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_test_conn(self, conn, cur, error_msg: Optional[str]):
        if error_msg:
            messagebox.showerror('Erro MySQL', error_msg)
            self.lbl_status['text'] = 'Falha na conexão.'
            return

        if conn:
            try:
                cur.execute('SELECT 1')
                messagebox.showinfo('Conexão', 'Conexão com MySQL OK!')
            except Exception as e:
                messagebox.showerror('Erro', f'Falha ao executar consulta de teste:\n{e}')
            finally:
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
            self.lbl_status['text'] = 'Pronto.'
        else:
            self.lbl_status['text'] = 'Falha na conexão.'

    def on_preview_importacao(self):
        if not self.state.path:
            messagebox.showwarning('Atenção', 'Selecione uma planilha primeiro.')
            return

        sheet = self.cmb_sheet.get() or 0
        carteira_label = self.cmb_carteira.get().strip()
        grupo_label = self.cmb_grupo.get().strip()
        carteira_id = self.carteiras_map.get(carteira_label)
        grupo_id = self.grupos_map.get(grupo_label)

        if carteira_id is None:
            messagebox.showwarning('Atenção', 'Selecione uma carteira.')
            return
        if grupo_id is None:
            messagebox.showwarning('Atenção', 'Selecione um grupo.')
            return

        try:
            self.lbl_status['text'] = 'Lendo planilha...'
            df = pd.read_excel(self.state.path, sheet_name=sheet)
            df = df.rename(columns=RENAME_MAP)
            df = normalizar_vazios_para_null(df)
            df['carteira'] = str(carteira_id)
            df['nomegrupo_id'] = int(grupo_id)

            for col in colunas_thproc:
                if col not in df.columns:
                    df[col] = None

            self.state.df = df
            self.state.empresa = ''
            self.state.sheet_name = str(sheet)
            self.state.carteira_id = carteira_id
            self.state.carteira_nome = carteira_label
            self.state.grupo_id = grupo_id
            self.state.grupo_nome = grupo_label
            self.state.missing_columns = []
            self._render_preview(df)
            self.lbl_status['text'] = f'Pré-visualização OK - {len(df)} linhas.'
        except KeyError as ke:
            messagebox.showerror('Erro de coluna', f'Coluna ausente na planilha: {ke}')
        except Exception as e:
            messagebox.showerror('Erro', f'Falha ao pré-visualizar:\n{e}')

    def on_preview(self):
        if not self.state.path:
            messagebox.showwarning('Atenção', 'Selecione uma planilha primeiro.')
            return
        sheet = self.cmb_sheet.get() or 0
        empresa = self.cmb_empresa.get()
        if not empresa:
            messagebox.showwarning('Atenção', 'Selecione a empresa para aplicar os presets.')
            return
        try:
            self.lbl_status['text'] = 'Lendo planilha...'
            df = pd.read_excel(self.state.path, sheet_name=sheet)
            df = df.rename(columns=RENAME_MAP)

            if usa_fluxo_regular(empresa):
                df = preparar_dataframe(df, empresa)
                self.state.missing_columns = []
                for col in colunas_thproc:
                    if col not in df.columns:
                        df[col] = None

                self.state.df = df
                self.state.empresa = empresa
                self._render_preview(df)
                self.lbl_status['text'] = f'PrÃ©-visualizaÃ§Ã£o OK â€“ {len(df)} linhas.'
                return

            # Validação obrigatória do campo natureza
            ok_natureza, linhas_invalidas, valores_invalidos = validar_campo_natureza(df)
            if not ok_natureza:
                msg = (
                    "A planilha não pode ser processada.\n\n"
                    "O campo 'natureza' deve conter apenas os valores:\n"
                    " - Judicial\n"
                    " - Administrativa\n\n"
                    f"Valores inválidos encontrados: {', '.join(valores_invalidos)}\n"
                )

                if linhas_invalidas:
                    msg += f"Linhas com erro: {', '.join(map(str, linhas_invalidas[:20]))}"
                    if len(linhas_invalidas) > 20:
                        msg += " ..."

                messagebox.showerror('Validação da Natureza', msg)
                self.lbl_status['text'] = 'Erro de validação na coluna natureza.'
                return

            ok_cnj, linhas_cnj_invalidas = validar_campo_cnj(df)
            if not ok_cnj:
                msg = (
                    "A planilha não pode ser processada.\n\n"
                    "O campo 'cnj' não pode começar com espaço em branco.\n"
                )
                if linhas_cnj_invalidas:
                    msg += f"\nLinhas com erro: {', '.join(map(str, linhas_cnj_invalidas[:20]))}"
                    if len(linhas_cnj_invalidas) > 20:
                        msg += " ..."

                messagebox.showerror('Validação do CNJ', msg)
                self.lbl_status['text'] = 'Erro de validação na coluna cnj.'
                return

            df = preparar_dataframe(df, empresa)

            ok, faltando = validar_colunas_para_insercao(df)
            self.state.missing_columns = faltando
            if not ok:
                messagebox.showwarning(
                    'Colunas ausentes',
                    f'Faltam colunas para o INSERT (irão como NULL na visualização):\n{faltando[:20]}...'
                )

            # Normalizações mínimas
            if not usa_fluxo_regular(empresa):
                if 'dataEvento' in df.columns:
                    df['dataEvento'] = pd.to_datetime(df['dataEvento']).dt.date
                if 'dataFase' in df.columns:
                    df['dataFase'] = pd.to_datetime(df['dataFase']).dt.date
                if 'dataContratacao' in df.columns:
                    df['dataContratacao'] = pd.to_datetime(df['dataContratacao']).dt.date

            # Garantir todas as colunas do INSERT
            for col in colunas_thproc:
                if col not in df.columns:
                    df[col] = OPTIONAL_INPUT_DEFAULTS.get(col)

            self.state.df = df
            self.state.empresa = empresa
            self._render_preview(df)
            self.lbl_status['text'] = f'Pré-visualização OK – {len(df)} linhas.'
        except KeyError as ke:
            messagebox.showerror('Erro de coluna', f'Coluna ausente na planilha: {ke}')
        except Exception as e:
            messagebox.showerror('Erro', f'Falha ao pré-visualizar:\n{e}')

    def _render_preview(self, df: pd.DataFrame, max_rows: int = 200):
        # Limpar
        for col in self.tree['columns']:
            self.tree.heading(col, text='')
        self.tree.delete(*self.tree.get_children())

        show_df = df.copy().head(max_rows)
        cols = list(show_df.columns)
        self.tree['columns'] = cols
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120, stretch=True)
        for _, row in show_df.iterrows():
            values = [str(row[c]) if pd.notna(row[c]) else '' for c in cols]
            self.tree.insert('', 'end', values=values)

    def on_send_importacao(self):
        if self.state.df is None or self.state.df.empty:
            messagebox.showwarning('Atenção', 'Faça a pré-visualização antes de enviar.')
            return

        df = self.state.df.copy()
        registros = montar_registros(df)

        self.pb['value'] = 0
        self.pb['maximum'] = len(registros)
        self.lbl_status['text'] = 'Enviando ao banco...'

        def progress_cb(done, total):
            self.after(0, lambda: self._update_progress(done, total))

        def worker():
            total, cnjs_duplicados, error_msg = inserir_em_lotes(
                registros,
                lote=500,
                progress_cb=progress_cb
            )
            self.after(0, lambda: self._finish_send(df, total, cnjs_duplicados, error_msg))

        threading.Thread(target=worker, daemon=True).start()

    def on_send(self):
        if self.state.df is None or self.state.df.empty:
            messagebox.showwarning('Atenção', 'Faça a pré-visualização antes de enviar.')
            return

        missing = self.state.missing_columns
        if missing:
            if not messagebox.askyesno(
                'Colunas faltando',
                f'Estas colunas não estão presentes e irão como NULL: {missing[:20]}...\nDeseja continuar?'
            ):
                return

        df = self.state.df.copy()

        if usa_fluxo_regular(self.state.empresa):
            registros = montar_registros(
                df,
                usar_defaults_opcionais=False,
                colunas=colunas_thproc_regular
            )

            self.pb['value'] = 0
            self.pb['maximum'] = len(registros)
            self.lbl_status['text'] = 'Enviando ao banco...'

            def progress_cb(done, total):
                self.after(0, lambda: self._update_progress(done, total))

            def worker():
                total, cnjs_duplicados, error_msg = inserir_em_lotes(
                    registros,
                    lote=500,
                    progress_cb=progress_cb,
                    colunas=colunas_thproc_regular
                )
                self.after(0, lambda: self._finish_send(df, total, cnjs_duplicados, error_msg))

            threading.Thread(target=worker, daemon=True).start()
            return

        ok_natureza, linhas_invalidas, valores_invalidos = validar_campo_natureza(df)
        if not ok_natureza:
            msg = (
                "O envio foi bloqueado.\n\n"
                "O campo 'natureza' deve conter apenas:\n"
                " - Judicial\n"
                " - Administrativa\n\n"
                f"Valores inválidos encontrados: {', '.join(valores_invalidos)}\n"
            )

            if linhas_invalidas:
                msg += f"Linhas com erro: {', '.join(map(str, linhas_invalidas[:20]))}"
                if len(linhas_invalidas) > 20:
                    msg += " ..."

            messagebox.showerror('Envio bloqueado', msg)
            self.lbl_status['text'] = 'Envio bloqueado por erro na coluna natureza.'
            return

        ok_cnj, linhas_cnj_invalidas = validar_campo_cnj(df)
        if not ok_cnj:
            msg = (
                "O envio foi bloqueado.\n\n"
                "O campo 'cnj' não pode começar com espaço em branco.\n"
            )
            if linhas_cnj_invalidas:
                msg += f"\nLinhas com erro: {', '.join(map(str, linhas_cnj_invalidas[:20]))}"
                if len(linhas_cnj_invalidas) > 20:
                    msg += " ..."

            messagebox.showerror('Envio bloqueado', msg)
            self.lbl_status['text'] = 'Envio bloqueado por erro na coluna cnj.'
            return

        registros = montar_registros(df)

        self.pb['value'] = 0
        self.pb['maximum'] = len(registros)
        self.lbl_status['text'] = 'Enviando ao banco...'

        def progress_cb(done, total):
            self.after(0, lambda: self._update_progress(done, total))

        def worker():
            total, cnjs_duplicados, error_msg = inserir_em_lotes(registros, lote=500, progress_cb=progress_cb)
            self.after(0, lambda: self._finish_send(df, total, cnjs_duplicados, error_msg))

        threading.Thread(target=worker, daemon=True).start()

    def _update_progress(self, done: int, total: int):
        self.pb['value'] = done
        self.lbl_status['text'] = f'Inserindo... {done}/{total}'
        self.update_idletasks()

    def _finish_send(
        self,
        df: pd.DataFrame,
        total: int,
        cnjs_duplicados: List[str],
        error_msg: Optional[str]
    ):
        if error_msg:
            self.lbl_status['text'] = 'Falha no envio.'
            messagebox.showerror('Erro MySQL', error_msg)
            return

        if cnjs_duplicados:
            messagebox.showinfo(
                'Duplicados Detectados',
                f'Encontrados {len(cnjs_duplicados)} processos duplicados.\n'
                f'Serão inseridos apenas {self.pb["maximum"] - len(cnjs_duplicados)} processos novos.\n'
                f'Uma planilha com os duplicados será gerada ao final.'
            )
            self._gerar_planilha_duplicados(df, cnjs_duplicados)

        self.pb['value'] = self.pb['maximum']
        self.lbl_status['text'] = f'Concluído. Inseridos {total} registros.'
        messagebox.showinfo('Finalizado', f'Inseridos {total} registros.')

    def _gerar_planilha_duplicados(self, df_original: pd.DataFrame, cnjs_duplicados: List[str]):
        """
        Gera planilha Excel com os processos duplicados encontrados.

        Args:
            df_original: DataFrame completo com todos os dados processados
            cnjs_duplicados: Lista de CNJs que já existem no banco
        """
        try:
            # Filtrar apenas as linhas com CNJ duplicado
            df_duplicados = df_original[df_original['cnj'].astype(str).isin(cnjs_duplicados)].copy()

            if df_duplicados.empty:
                messagebox.showinfo('Aviso', 'Nenhum processo duplicado para exportar.')
                return

            # Adicionar coluna indicando que são duplicados
            df_duplicados.insert(0, 'STATUS', 'DUPLICADO')
            df_duplicados.insert(1, 'DATA_VERIFICACAO', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

            # Solicitar local para salvar
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            arquivo_saida = filedialog.asksaveasfilename(
                title='Salvar planilha de processos duplicados',
                defaultextension='.xlsx',
                filetypes=[('Arquivos Excel', '*.xlsx')],
                initialfile=f'processos_duplicados_{timestamp}.xlsx'
            )

            if not arquivo_saida:
                messagebox.showinfo(
                    'Aviso',
                    f'{len(df_duplicados)} processos duplicados não foram salvos em planilha.\n'
                    'Você cancelou a operação.'
                )
                return

            # Criar o Excel com formatação
            with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
                # Aba principal com duplicados
                df_duplicados.to_excel(writer, index=False, sheet_name='Duplicados')

                # Aba de resumo
                resumo = pd.DataFrame({
                    'Métrica': [
                        'Total de Processos Duplicados',
                        'Data de Verificação',
                        'Empresa/Cliente',
                        'Arquivo Original'
                    ],
                    'Valor': [
                        len(df_duplicados),
                        datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                        self.state.empresa if self.state.empresa else 'Não informado',
                        self.state.path if self.state.path else 'Não informado'
                    ]
                })
                resumo.to_excel(writer, index=False, sheet_name='Resumo')

                # Aba com lista simples de CNJs
                df_cnjs = pd.DataFrame({
                    'CNJ': cnjs_duplicados,
                    'Observação': 'Já existe no banco de dados'
                })
                df_cnjs.to_excel(writer, index=False, sheet_name='Lista_CNJs')

            # Mensagem de sucesso
            messagebox.showinfo(
                'Planilha Gerada com Sucesso',
                f'Planilha de duplicados salva com sucesso!\n\n'
                f'📁 Arquivo: {arquivo_saida}\n'
                f'📊 Total de processos duplicados: {len(df_duplicados)}\n'
                f'📋 Abas criadas:\n'
                f'   • Duplicados (dados completos)\n'
                f'   • Resumo (estatísticas)\n'
                f'   • Lista_CNJs (CNJs duplicados)'
            )

            # Atualizar status
            self.lbl_status['text'] = f'Planilha de duplicados gerada: {len(df_duplicados)} processos'

        except PermissionError:
            messagebox.showerror(
                'Erro de Permissão',
                'Não foi possível salvar o arquivo.\n'
                'Verifique se o arquivo está aberto em outro programa.'
            )
        except Exception as e:
            messagebox.showerror(
                'Erro ao Gerar Planilha',
                f'Erro ao gerar planilha de duplicados:\n{e}'
            )

