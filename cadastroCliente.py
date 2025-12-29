from __future__ import annotations
import os
import socket
import threading
from dataclasses import dataclass
from typing import Optional, Tuple, List

import tkinter as tk
from tkinter import ttk, messagebox

import mysql.connector
from dotenv import load_dotenv

from pathlib import Path
import sys
import re
import requests

# =========================
# .env / Conexão
# =========================
LAST_ENV_PATH = None

def carregar_variaveis_ambiente():
    global LAST_ENV_PATH
    load_dotenv(override=False)
    candidates: List[Path] = []
    try:
        if getattr(sys, 'frozen', False):
            base_dir = Path(sys.executable).parent
            meipass = Path(getattr(sys, '_MEIPASS', base_dir))
            candidates += [base_dir / '.env', meipass / '.env']
        else:
            base_dir = Path(__file__).parent
            candidates += [base_dir / '.env']
    except Exception:
        pass
    candidates.append(Path.cwd() / '.env')
    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=p, override=True)
            LAST_ENV_PATH = str(p)
            break

def obter_config_banco() -> dict:
    carregar_variaveis_ambiente()
    cfg = {
        'host': os.getenv('DB_HOST', ''),
        'user': os.getenv('DB_USER', ''),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_DATABASE', ''),
        'port': int(os.getenv('DB_PORT', '3306')),
        'timeout': int(os.getenv('DB_TIMEOUT', '15')),
    }
    ap = os.getenv('DB_AUTH_PLUGIN', '').strip()
    if ap:
        cfg['auth_plugin'] = ap
    for envk, dstk in (('DB_SSL_CA', 'ssl_ca'), ('DB_SSL_CERT', 'ssl_cert'), ('DB_SSL_KEY', 'ssl_key')):
        v = os.getenv(envk, '').strip()
        if v:
            cfg[dstk] = v
    return cfg

def teste_tcp(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except Exception as e:
        return False, str(e)

def conectar_ao_mysql() -> Tuple[Optional[object], Optional[object], Optional[str]]:
    cfg = obter_config_banco()
    ok, err = teste_tcp(cfg['host'], cfg['port'], timeout=min(cfg.get('timeout', 15), 5))
    if not ok:
        return None, None, f"Falha TCP em {cfg['host']}:{cfg['port']} -> {err}"

    def base_kwargs_connector():
        kw = dict(
            host=cfg['host'],
            user=cfg['user'],
            password=cfg['password'],
            database=cfg['database'],
            port=cfg['port'],
            connection_timeout=cfg.get('timeout', 15),
            use_pure=True,
            charset="utf8mb4",
        )
        for k in ('ssl_ca', 'ssl_cert', 'ssl_key'):
            if k in cfg:
                kw[k] = cfg[k]
        return kw

    try:
        conn = mysql.connector.connect(**base_kwargs_connector())
        conn.autocommit = False
        return conn, conn.cursor(), None
    except mysql.connector.Error as err1:
        low = str(err1).lower()
        if 'authentication plugin' in low and 'mysql_native_password' in low and 'not supported' in low:
            try:
                import pymysql
            except ImportError:
                return None, None, ("PyMySQL não instalado para fallback.\nRode: python -m pip install PyMySQL")
            try:
                ssl_params = None
                if 'ssl_ca' in cfg or 'ssl_cert' in cfg or 'ssl_key' in cfg:
                    ssl_params = {}
                    if 'ssl_ca' in cfg:   ssl_params['ca']   = cfg['ssl_ca']
                    if 'ssl_cert' in cfg: ssl_params['cert'] = cfg['ssl_cert']
                    if 'ssl_key' in cfg:  ssl_params['key']  = cfg['ssl_key']
                conn = pymysql.connect(
                    host=cfg['host'],
                    user=cfg['user'],
                    password=cfg['password'],
                    database=cfg['database'],
                    port=cfg['port'],
                    connect_timeout=cfg.get('timeout', 15),
                    ssl=ssl_params,
                    autocommit=False,
                    cursorclass=pymysql.cursors.Cursor,
                    charset="utf8mb4",
                )
                return conn, conn.cursor(), None
            except Exception as err2:
                return None, None, f"Falha no fallback PyMySQL: {err2}"
        return None, None, f"Erro MySQL: {err1}"
    except Exception as e:
        return None, None, f"Erro inesperado: {e}"

# =========================
# Utils CNPJ / APIs
# =========================
def apenas_digitos(s: str) -> str:
    return re.sub(r'\D+', '', s or '')

def tipo_doc(s:str) -> Optional[str]:
    d = apenas_digitos(s)
    if len(d) == 14:
        return "CNPJ"
    if len(d) == 11:
        return "CPF"
    return None

def formatar_cpf(cpf: str) -> str:
    d = apenas_digitos(cpf)
    if len(d) != 11:
        return cpf.strip()
    return f"{d[0:3]}.{d[3:6]}.{d[6:9]}-{d[9:11]}"

def formatar_doc(doc: str) -> str:
    t = tipo_doc(doc)
    if t == "CNPJ":
        return formatar_cnpj(doc)
    if t == "CPF":
        return formatar_cpf(doc)
    return doc.strip()

def validar_cpf(cpf: str) -> bool:
    d = apenas_digitos(cpf)
    if len(d) != 11 or len(set(d)) == 1:
        return False
    # DV1
    s = sum(int(n) * p for n, p in zip(d[:9], range(10, 1, -1)))
    dv1 = (s * 10) % 11
    dv1 = 0 if dv1 == 10 else dv1
    # DV2
    s = sum(int(n) * p for n, p in zip(d[:9] + str(dv1), range(11, 1, -1)))
    dv2 = (s * 10) % 11
    dv2 = 0 if dv2 == 10 else dv2
    return d[-2:] == f"{dv1}{dv2}"

def formatar_cnpj(cnpj: str) -> str:
    d = apenas_digitos(cnpj)
    if len(d) != 14:
        return cnpj.strip()
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def _calc_dv(base: str) -> int:
    pesos = list(range(len(base) - 7, 1, -1))
    soma = sum(int(d) * p for d, p in zip(base, pesos))
    r = 11 - (soma % 11)
    return 0 if r >= 10 else r

def validar_cnpj(cnpj: str) -> bool:
    d = apenas_digitos(cnpj)
    if len(d) != 14 or len(set(d)) == 1:
        return False
    dv1 = _calc_dv(d[:12])
    dv2 = _calc_dv(d[:12] + str(dv1))
    return d[-2:] == f"{dv1}{dv2}"

def _get_brasilapi_cnpj(cnpj: str, timeout: float = 8.0) -> Optional[str]:
    d = apenas_digitos(cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{d}"
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            data = r.json()
            razao = data.get("razao_social") or data.get("razaoSocial")
            if razao:
                return razao.strip()
    except Exception:
        pass
    return None

def _get_receitaws_cnpj(cnpj: str, timeout: float = 8.0) -> Optional[str]:
    d = apenas_digitos(cnpj)
    url = f"https://www.receitaws.com.br/v1/cnpj/{d}"
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and data.get("status") == "ERROR":
                return None
            nome = data.get("nome")
            if nome:
                return nome.strip()
    except Exception:
        pass
    return None

def obter_razao_social(doc: str) -> Tuple[bool, str, str]:
    t = tipo_doc(doc)
    fmt = formatar_doc(doc)
    if t == "CNPJ":

        #if not validar_cnpj(cnpj):
        #    return False, "CNPJ inválido (DV).", formatar_cnpj(cnpj)
        razao = _get_brasilapi_cnpj(doc) or _get_receitaws_cnpj(doc)

        if razao:
            return True, razao, fmt
        return False, "Não encontrado ou serviço indisponível.", fmt

    if t == "CPF":
        return False, "Para CPF não há consulta automática. Informe o nome e prossiga.", fmt

    return False, "Documento deve ter 11 (CPF) ou 14 (CNPJ) dígitos.", fmt

# =========================
# Lógica de Cadastro/Atualização
# =========================
def listar_equipes() -> Tuple[bool, List[str], str]:
    conn, cur, err = conectar_ao_mysql()
    if not conn:
        return False, [], err or "Falha na conexão."
    try:
        cur.execute("SELECT name FROM auth_group ORDER BY name ASC")
        nomes = [r[0] for r in cur.fetchall()]
        return True, nomes, ""
    except Exception as e:
        return False, [], f"Erro ao listar equipes: {e}"
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

def obter_codequipe(cursor, nome_equipe: str) -> Optional[int]:
    cursor.execute("SELECT id FROM auth_group WHERE name = %s", (nome_equipe.strip(),))
    row = cursor.fetchone()
    return int(row[0]) if row else None

def obter_id_grupo_por_nome(cursor, codequipe: int, nome_grupo: str) -> Optional[int]:
    cursor.execute(
        "SELECT id FROM thgrupocli WHERE codequipe = %s AND nomegrupo = %s",
        (codequipe, nome_grupo.strip())
    )
    row = cursor.fetchone()
    return int(row[0]) if row else None

def obter_ultimo_id_grupo(cursor, codequipe: int) -> Optional[int]:
    cursor.execute(
        "SELECT id FROM thgrupocli WHERE codequipe = %s ORDER BY id DESC LIMIT 1",
        (codequipe,)
    )
    row = cursor.fetchone()
    return int(row[0]) if row else None

def inserir_grupo(cursor, novo_id: int, codequipe: int, nome_grupo: str):
    cursor.execute(
        "INSERT INTO thgrupocli (id, codequipe, nomegrupo) VALUES (%s, %s, %s)",
        (novo_id, codequipe, nome_grupo.strip())
    )

def inserir_cliente(cursor, id_grupo: int, nome_cliente: str, cnpj: str):
    doc_fmt = formatar_cnpj(cnpj)
    cursor.execute(
        "INSERT INTO thcliente (grupo_id, name_cli, cnpj) VALUES (%s, %s, %s)",
        (id_grupo, nome_cliente.strip().upper(), doc_fmt)
    )

def cadastrar_cliente(nome_equipe: str, nome_grupo: str, nome_cliente: str, cnpj: str) -> Tuple[bool, str]:
    if not (nome_equipe and nome_grupo and nome_cliente and cnpj):
        return False, "Preencha todos os campos."
   # if not validar_cnpj(cnpj):
   #     return False, "CNPJ inválido (verifique dígitos)."

    conn, cur, err = conectar_ao_mysql()
    if not conn:
        return False, err or "Falha na conexão com o banco."

    try:
        codequipe = obter_codequipe(cur, nome_equipe)
        if codequipe is None:
            return False, f"Equipe não encontrada: {nome_equipe}"

        id_grupo = obter_id_grupo_por_nome(cur, codequipe, nome_grupo)

        if id_grupo is not None:
            inserir_cliente(cur, id_grupo, nome_cliente, cnpj)
            conn.commit()
            return True, (
                f"Cliente inserido no grupo existente.\n"
                f"Equipe={nome_equipe} (id {codequipe})\n"
                f"Grupo={nome_grupo} (id {id_grupo})"
            )
        else:
            ultimo = obter_ultimo_id_grupo(cur, codequipe)
            novo_id = (ultimo or 0) + 1
            inserir_grupo(cur, novo_id, codequipe, nome_grupo)
            inserir_cliente(cur, novo_id, nome_cliente, cnpj)
            conn.commit()
            return True, (
                f"Grupo criado e cliente inserido com sucesso.\n"
                f"Equipe={nome_equipe} (id {codequipe})\n"
                f"Grupo={nome_grupo} (id {novo_id})"
            )

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"Erro durante o cadastro: {e}"
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


def atualizar_nome_cliente(cnpj: str, novo_nome: str) -> Tuple[bool, str, int]:
    """
    Atualiza name_cli pelo CNPJ (aceita com/sem máscara).
    Retorna (ok, msg, rows_afetadas).
    """
    if not (cnpj and novo_nome):
        return False, "Informe CNPJ e Nome do cliente.", 0
    # if not validar_cnpj(cnpj):
    #    return False, "CNPJ inválido (verifique dígitos).", 0

    # 1. Remove os caracteres de máscara do CNPJ de entrada
    d_apenas_digitos = apenas_digitos(cnpj)

    # 2. CORREÇÃO: Cria a string de busca com curingas
    # Usa f-string para criar o padrão LIKE '%%' + digitos + '%%'
    cnpj_like_pattern = f"%{d_apenas_digitos}%"

    conn, cur, err = conectar_ao_mysql()
    if not conn:
        return False, err or "Falha na conexão com o banco.", 0

    try:
        sql = """
        UPDATE thcliente
        SET name_cli = %s
        WHERE REPLACE(REPLACE(REPLACE(cnpj,'.',''),'-',''),'/','') LIKE %s
        """
        # Passa a string com o padrão LIKE como segundo parâmetro
        cur.execute(sql, (novo_nome.strip().upper(), cnpj_like_pattern))
        conn.commit()
        rows = cur.rowcount or 0

        # A mensagem de "nenhum registro encontrado" foi ajustada para refletir a busca parcial
        if rows == 0:
            return False, f"Nenhum registro encontrado que contenha a sequência '{d_apenas_digitos}' no CNPJ.", 0
        return True, f"Nome atualizado com sucesso ({rows} registro(s)).", rows

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"Erro ao atualizar: {e}", 0
    finally:
        try:
            cur.close();
            conn.close()
        except Exception:
            pass



# =========================
# UI Tkinter
# =========================
@dataclass
class FormState:
    equipe: str = ""
    grupo: str = ""
    cliente: str = ""
    cnpj: str = ""

class CadastroClienteApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Cadastro de Clientes")
        self.geometry("760x440")
        self.resizable(False, False)

        self.state = FormState()
        self._build_ui()
        self.after(300, self._load_equipes_async)

    def _build_ui(self):
        outer = ttk.Frame(self, padding=16)
        outer.pack(fill="both", expand=True)

        row = 0
        ttk.Label(outer, text="Nome da Equipe:").grid(row=row, column=0, sticky="w", pady=6)
        equipe_row = ttk.Frame(outer)
        equipe_row.grid(row=row, column=1, columnspan=3, sticky="we", pady=6)
        self.cmb_equipe = ttk.Combobox(equipe_row, values=[], state="readonly", width=50)
        self.cmb_equipe.pack(side="left", fill="x", expand=True)
        ttk.Button(equipe_row, text="Recarregar", command=self._load_equipes_async).pack(side="left", padx=6)

        row += 1
        ttk.Label(outer, text="Nome do grupo:").grid(row=row, column=0, sticky="w", pady=6)
        self.in_grupo = ttk.Entry(outer, width=50)
        self.in_grupo.grid(row=row, column=1, sticky="w", pady=6, columnspan=3)

        row += 1
        ttk.Label(outer, text="Nome do cliente:").grid(row=row, column=0, sticky="w", pady=6)
        self.in_cliente = ttk.Entry(outer, width=50)
        self.in_cliente.grid(row=row, column=1, sticky="w", pady=6, columnspan=3)

        row += 1
        ttk.Label(outer, text="CNPJ/CPF:").grid(row=row, column=0, sticky="w", pady=6)
        self.in_cnpj = ttk.Entry(outer, width=28)
        self.in_cnpj.grid(row=row, column=1, sticky="w", pady=6)

        self.btn_buscar = ttk.Button(outer, text="Buscar Razão Social", command=self.on_buscar_razao)
        self.btn_buscar.grid(row=row, column=2, sticky="w", padx=6, pady=6)

        # Novo botão: Atualizar Nome
        self.btn_atualizar = ttk.Button(outer, text="Atualizar Nome", command=self.on_update_name)
        self.btn_atualizar.grid(row=row, column=3, sticky="w", padx=6, pady=6)

        # Ações principais
        row += 1
        btns = ttk.Frame(outer)
        btns.grid(row=row, column=0, columnspan=4, pady=(16, 8), sticky="w")
        ttk.Button(btns, text="Testar Conexão", command=self.on_test_conn).pack(side="left")
        ttk.Button(btns, text="Cadastrar", command=self.on_submit).pack(side="left", padx=8)

        # Status
        row += 1
        self.lbl_status = ttk.Label(outer, text="Pronto.")
        self.lbl_status.grid(row=row, column=0, columnspan=4, sticky="w", pady=(6, 0))

        outer.grid_columnconfigure(1, weight=1)

    # ----- carregar equipes -----
    def _load_equipes_async(self):
        self._set_status("Carregando equipes...")
        def worker():
            ok, nomes, msg = listar_equipes()
            def finish():
                if ok:
                    self.cmb_equipe["values"] = nomes
                    if nomes and not self.cmb_equipe.get():
                        self.cmb_equipe.set(nomes[0])
                    self._set_status(f"{len(nomes)} equipes carregadas.")
                else:
                    messagebox.showerror("Erro", msg)
                    self._set_status("Falha ao carregar equipes.")
            self.after(0, finish)
        threading.Thread(target=worker, daemon=True).start()

    # ---------- Ações ----------
    def on_test_conn(self):
        self._set_status("Testando conexão...")
        def worker():
            conn, cur, err = conectar_ao_mysql()
            def finish():
                if conn:
                    try:
                        cur.execute("SELECT 1")
                        messagebox.showinfo("Conexão", "Conexão com MySQL OK!")
                    except Exception as e:
                        messagebox.showerror("Erro", f"Falha no SELECT 1:\n{e}")
                    finally:
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                    self._set_status("Pronto.")
                else:
                    messagebox.showerror("Conexão", err or "Falha ao conectar.")
                    self._set_status("Falha na conexão.")
            self.after(0, finish)
        threading.Thread(target=worker, daemon=True).start()

    def on_buscar_razao(self):
        cnpj = self.in_cnpj.get().strip()
        if not cnpj:
            messagebox.showwarning("Atenção", "Informe o CNPJ para buscar.")
            return
        self._set_status("Consultando razão social...")
        self.btn_buscar.config(state="disabled")

        def worker():
            ok, resp, fmt = obter_razao_social(cnpj)
            def finish():
                self.in_cnpj.delete(0, tk.END)
                self.in_cnpj.insert(0, fmt)
                if ok:
                    self.in_cliente.delete(0, tk.END)
                    self.in_cliente.insert(0, resp)
                    messagebox.showinfo("Razão Social", f"Encontrado:\n{resp}")
                    self._set_status("Razão social preenchida.")
                else:
                    messagebox.showwarning("Não encontrado", resp)
                    self._set_status("Não encontrado / indisponível.")
                self.btn_buscar.config(state="normal")
            self.after(0, finish)
        threading.Thread(target=worker, daemon=True).start()

    def on_update_name(self):
        """Atualiza name_cli pelo CNPJ informado."""
        cnpj = self.in_cnpj.get().strip()
        nome = self.in_cliente.get().strip()
        if not (cnpj and nome):
            messagebox.showwarning("Atenção", "Preencha CNPJ e Nome do cliente para atualizar.")
            return

        self._set_status("Atualizando nome...")
        self.btn_atualizar.config(state="disabled")
        def worker():
            ok, msg, rows = atualizar_nome_cliente(cnpj, nome)
            def finish():
                if ok:
                    messagebox.showinfo("Sucesso", msg)
                    self._set_status("Nome atualizado.")
                else:
                    messagebox.showerror("Erro", msg)
                    self._set_status("Falha ao atualizar.")
                self.btn_atualizar.config(state="normal")
            self.after(0, finish)
        threading.Thread(target=worker, daemon=True).start()

    def on_submit(self):
        equipe  = self.cmb_equipe.get().strip()
        grupo   = self.in_grupo.get().strip()
        cliente = self.in_cliente.get().strip().upper()
        cnpj    = self.in_cnpj.get().strip()

        if not (equipe and grupo and cliente and cnpj):
            messagebox.showwarning("Atenção", "Preencha todos os campos.")
            return

        self._set_status("Cadastrando...")
        def worker():
            ok, msg = cadastrar_cliente(equipe, grupo, cliente, cnpj)
            def finish():
                if ok:
                    messagebox.showinfo("Sucesso", msg)
                    self._set_status("Cadastro concluído.")
                else:
                    messagebox.showerror("Erro", msg)
                    self._set_status("Erro no cadastro.")
            self.after(0, finish)
        threading.Thread(target=worker, daemon=True).start()

    def _set_status(self, txt: str):
        self.lbl_status.configure(text=txt)

# =========================
# Main
# =========================
if __name__ == "__main__":
    app = CadastroClienteApp()
    app.mainloop()
