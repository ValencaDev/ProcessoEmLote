from __future__ import annotations

import re
import socket
from typing import List, Optional, Sequence, Tuple

import mysql.connector
import pandas as pd

from pel_config import LAST_ENV_PATH, colunas_thproc, obter_config_banco

def teste_tcp(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except Exception as e:
        return False, str(e)

def traduzir_erro_mysql(err: Exception | str) -> str:
    """Traduz mensagens comuns do MySQL para portugues."""
    mensagem_original = str(err).strip()
    mensagem_lower = mensagem_original.lower()
    codigo = None

    if hasattr(err, 'errno'):
        try:
            codigo = int(getattr(err, 'errno'))
        except (TypeError, ValueError):
            codigo = None

    match_codigo = re.match(r"^(\d+)\s*\([^)]+\):", mensagem_original)
    if match_codigo:
        try:
            codigo = int(match_codigo.group(1))
        except ValueError:
            pass

    if codigo == 1452 or 'cannot add or update a child row' in mensagem_lower:
        coluna = None
        tabela_ref = None
        constraint = None

        match_constraint = re.search(r"CONSTRAINT\s+`([^`]+)`", mensagem_original, re.IGNORECASE)
        if match_constraint:
            constraint = match_constraint.group(1)

        match_coluna = re.search(r"FOREIGN KEY\s+\(`([^`]+)`\)", mensagem_original, re.IGNORECASE)
        if match_coluna:
            coluna = match_coluna.group(1)

        match_tabela = re.search(r"REFERENCES\s+`([^`]+)`", mensagem_original, re.IGNORECASE)
        if match_tabela:
            tabela_ref = match_tabela.group(1)

        detalhes = []
        if coluna:
            detalhes.append(f"Campo relacionado: {coluna}.")
        if tabela_ref:
            detalhes.append(f"Tabela de referencia: {tabela_ref}.")
        if constraint:
            detalhes.append(f"Restricao: {constraint}.")

        return (
            "Nao foi possivel inserir ou atualizar o registro porque o valor informado "
            "em uma chave estrangeira nao existe na tabela relacionada.\n\n"
            + (" ".join(detalhes) + "\n\n" if detalhes else "")
            + "Revise se o grupo, cliente ou outro cadastro vinculado ja existe no banco antes de reenviar."
        )

    if codigo == 1062 or 'duplicate entry' in mensagem_lower:
        return (
            "Ja existe um registro com os mesmos dados em um campo que precisa ser unico.\n\n"
            "Revise duplicidades antes de reenviar."
        )

    if codigo == 1048 or 'cannot be null' in mensagem_lower:
        match_coluna = re.search(r"Column\s+'([^']+)'", mensagem_original, re.IGNORECASE)
        coluna = match_coluna.group(1) if match_coluna else 'obrigatoria'
        return f"O campo {coluna} e obrigatorio e nao pode ficar vazio."

    if codigo == 1406 or 'data too long for column' in mensagem_lower:
        match_coluna = re.search(r"Data too long for column\s+'([^']+)'", mensagem_original, re.IGNORECASE)
        coluna = match_coluna.group(1) if match_coluna else 'informado'
        return f"O valor enviado para a coluna {coluna} ultrapassa o tamanho permitido."

    if codigo == 1054 or 'unknown column' in mensagem_lower:
        match_coluna = re.search(r"Unknown column\s+'([^']+)'", mensagem_original, re.IGNORECASE)
        coluna = match_coluna.group(1) if match_coluna else 'informada'
        return f"A coluna {coluna} nao existe no banco de dados."

    if codigo == 1146 or "doesn't exist" in mensagem_lower:
        match_tabela = re.search(r"Table\s+'([^']+)'", mensagem_original, re.IGNORECASE)
        tabela = match_tabela.group(1) if match_tabela else 'informada'
        return f"A tabela {tabela} nao foi encontrada no banco de dados."

    if codigo == 2003 or "can't connect to mysql server" in mensagem_lower:
        return (
            "Nao foi possivel conectar ao servidor MySQL.\n\n"
            "Verifique se o servidor esta online, se a porta esta liberada e se a rede permite a conexao."
        )

    if 'authentication plugin' in mensagem_lower and 'not supported' in mensagem_lower:
        return "O metodo de autenticacao exigido pelo servidor MySQL nao e suportado pela conexao atual."

    return mensagem_original

def conectar_ao_mysql() -> Tuple[object, object]:
    """Conecta ao MySQL.
    1) Pré-testa a porta TCP (diagnóstico de 2003).
    2) Tenta mysql-connector (sem forçar plugin).
    3) Se aparecer 'mysql_native_password is not supported', faz fallback para PyMySQL.
    """
    cfg = obter_config_banco()

    # 1) Pré-teste de rede/porta
    ok, err = teste_tcp(cfg['host'], cfg['port'], timeout=min(cfg.get('timeout', 15), 5))
    if not ok:
        raise RuntimeError(
            f"Não foi possível abrir TCP para {cfg['host']}:{cfg['port']}\n"
            f"Motivo: {err}\n\nVerifique firewall/NAT/segurança do servidor e se o MySQL está escutando."
        )

    # kwargs-base SEM auth_plugin explícito
    def base_kwargs_connector():
        kw = dict(
            host=cfg['host'],
            user=cfg['user'],
            password=cfg['password'],
            database=cfg['database'],
            port=cfg['port'],
            connection_timeout=cfg.get('timeout', 15),
            use_pure=True,
        )
        # SSL opcional via .env
        for k in ('ssl_ca', 'ssl_cert', 'ssl_key'):
            if k in cfg:
                kw[k] = cfg[k]
        return kw

    # 2) Tenta o mysql-connector
    try:
        conn = mysql.connector.connect(**base_kwargs_connector())
        return conn, conn.cursor()
    except mysql.connector.Error as err1:
        msg1 = str(err1)
        low = msg1.lower()

        # Caso clássico deste seu log: cliente diz que 'mysql_native_password' não é suportado
        if 'authentication plugin' in low and 'mysql_native_password' in low and 'not supported' in low:
            # 3) Fallback para PyMySQL (suporta mysql_native_password)
            try:
                import pymysql
            except ImportError:
                raise RuntimeError(
                    'PyMySQL não está instalado.\n\nRode:\n'
                    '"C:\\Users\\Renan Farias\\AppData\\Local\\Programs\\Python\\Python313\\python.exe" -m pip install PyMySQL'
                ) from err1

            try:
                # Monta SSL (se fornecido no .env)
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
                    ssl=ssl_params
                )
                return conn, conn.cursor()
            except Exception as err2:
                raise RuntimeError(
                    f'Falha no fallback PyMySQL:\n{traduzir_erro_mysql(err2)}\n\n'
                    f'Host={cfg.get("host")}\nDB={cfg.get("database")}\nPort={cfg.get("port")}\n'
                    f'.env usado: {LAST_ENV_PATH or "NÃO ENCONTRADO"}'
                ) from err2

        # Outros erros do connector: mostra e sai
        raise RuntimeError(
            f"{traduzir_erro_mysql(err1)}\n\nHost={cfg.get('host')}\nDB={cfg.get('database')}\nPort={cfg.get('port')}\n"
            f".env usado: {LAST_ENV_PATH or 'NÃO ENCONTRADO'}"
        ) from err1
    except Exception as e:
        raise RuntimeError(traduzir_erro_mysql(e)) from e


def listar_carteiras() -> Tuple[bool, List[Tuple[int, str]], str]:
    """Lista as carteiras disponiveis em auth_group."""
    conn = None
    cur = None
    try:
        conn, cur = conectar_ao_mysql()
        cur.execute("SELECT id, name FROM auth_group ORDER BY name ASC")
        return True, [(int(row[0]), str(row[1])) for row in cur.fetchall()], ""
    except Exception as e:
        return False, [], f"Erro ao listar carteiras: {e}"
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def listar_grupos_por_carteira(carteira_id: int) -> Tuple[bool, List[Tuple[int, str]], str]:
    """Lista os grupos da thgrupocli vinculados a uma carteira."""
    conn = None
    cur = None
    try:
        conn, cur = conectar_ao_mysql()
        cur.execute(
            "SELECT id, nomegrupo FROM thgrupocli WHERE codequipe = %s ORDER BY nomegrupo ASC",
            (int(carteira_id),)
        )
        return True, [(int(row[0]), str(row[1])) for row in cur.fetchall()], ""
    except Exception as e:
        return False, [], f"Erro ao listar grupos: {e}"
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def normalizar_texto(serie: pd.Series, max_len: Optional[int] = None, strip: bool = False) -> pd.Series:
    """Converte textos preservando valores nulos para evitar inserir 'nan' literal."""
    texto = serie.astype('string')
    if strip:
        texto = texto.str.strip()
    if max_len is not None:
        texto = texto.str.slice(0, max_len)
    return texto.replace({'': pd.NA})


def formatar_datas_e_numeros(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza campos monetários/datas e limita textos para evitar erros no INSERT."""
    # Monetários
    for campo in ['valorFinalCausa','valorCausa','valorProvisionado']:
        if campo in df.columns:
            df[campo] = pd.to_numeric(df[campo], errors='coerce').fillna(0.0)

    if 'numeroUnidade' in df.columns:
        df['numeroUnidade'] = pd.to_numeric(df['numeroUnidade'], errors='coerce')

    # Datas
    for col in COLUNAS_DATA:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            df[col] = df[col].fillna(pd.to_datetime(date.today()))
            df[col] = df[col].dt.date

    # Limite de 255 chars
    for campo in CAMPOS_TEXTO_255:
        if campo in df.columns:
            df[campo] = normalizar_texto(df[campo], max_len=255)

    # Fallbacks úteis
    if 'tipoObjeto' in df.columns:
        df['tipoObjeto'] = df['tipoObjeto'].fillna('Não Informado')
    if 'tipoAcao' in df.columns:
        df['tipoAcao'] = df['tipoAcao'].fillna('Não Informando')
    else:
        df['tipoAcao'] = 'Não Informando'
    if 'fase' in df.columns:
        df['fase'] = df['fase'].fillna('Conhecimento')
    if 'descricaoEvento' in df.columns:
        df['descricaoEvento'] = df['descricaoEvento'].fillna('Não Informado')
    if 'solicitanteEvento' in df.columns:
        df['solicitanteEvento'] = df['solicitanteEvento'].fillna('438')
    if 'responsavelEvento' in df.columns:
        df['responsavelEvento'] = df['responsavelEvento'].fillna('438')

    # Datas com hora → manter só data se a coluna existir
    if 'data_hora_submit' in df.columns:
        df['data_hora_submit'] = pd.to_datetime(df['data_hora_submit'], errors='coerce', dayfirst=True)
        df['data_hora_submit'] = df['data_hora_submit'].fillna(pd.to_datetime(date.today()))
        df['data_hora_submit'] = df['data_hora_submit'].dt.date

    # CNJ como string
    if 'cnj' in df.columns:
        df['cnj'] = normalizar_texto(df['cnj'], strip=True)

    return df

def usa_fluxo_regular(empresa: str) -> bool:
    """Indica se a equipe deve manter os dados da planilha sem presets."""
    return empresa == EMPRESA_REGULAR

def normalizar_vazios_para_null(df: pd.DataFrame) -> pd.DataFrame:
    """Converte strings vazias/brancas em NULL sem alterar os demais valores."""
    df = df.copy()
    for coluna in df.columns:
        serie = df[coluna]
        if pd.api.types.is_object_dtype(serie) or pd.api.types.is_string_dtype(serie):
            texto = serie.astype('string')
            mascara_vazia = texto.str.strip().eq('')
            df[coluna] = serie.mask(mascara_vazia, pd.NA)
    return df

def aplicar_presets(df: pd.DataFrame, empresa: str) -> pd.DataFrame:
    """Aplica defaults comuns e presets específicos por empresa."""
    # Defaults comuns: preenche nulos ou cria coluna
    for k, v in COMMON_DEFAULTS.items():
        if k in df.columns:
            df[k] = df[k].fillna(v)
        else:
            df[k] = v

    # Presets específicos
    preset = COMPANY_PRESETS.get(empresa, {})
    for k, v in preset.items():
        if v is None and k == 'descricaoObjeto' and 'tipoObjeto' in df.columns:
            df['descricaoObjeto'] = df['tipoObjeto']
        else:
            df[k] = v  # força valor do preset
    return df

def preparar_dataframe(df: pd.DataFrame, empresa: str) -> pd.DataFrame:
    """Prepara o DataFrame para preview/envio conforme a equipe selecionada."""
    if usa_fluxo_regular(empresa):
        df = normalizar_vazios_para_null(df)
        df['carteira'] = '46'
        if 'especialidade' in df.columns:
            df['especialidade'] = df['especialidade'].fillna(' ')
        else:
            df['especialidade'] = ' '
        df['verificado'] = 0
        df['exportado'] = 0
        df['excluido'] = 0
        return df

    df = aplicar_presets(df, empresa)
    df = formatar_datas_e_numeros(df)
    return df

def validar_colunas_para_insercao(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Valida se todas as colunas necessárias para o INSERT existem no DataFrame."""
    faltando = [
        c for c in colunas_thproc
        if c not in df.columns and c not in OPTIONAL_INPUT_COLUMNS
    ]
    return (len(faltando) == 0), faltando

def validar_campo_natureza(df: pd.DataFrame) -> Tuple[bool, List[int], List[str]]:
    """
    Valida se a coluna 'natureza' contém apenas:
    - Judicial
    - Administrativa

    Retorna:
        (ok, linhas_invalidas, valores_invalidos)
    """
    if 'natureza' not in df.columns:
        return False, [], ['COLUNA_NATUREZA_AUSENTE']

    valores_validos = {'Judicial', 'Administrativa'}

    serie = df['natureza'].fillna('').astype(str).str.strip()

    mascara_invalida = ~serie.isin(valores_validos)
    linhas_invalidas = (df.index[mascara_invalida] + 2).tolist()  # +2 por causa do cabeçalho Excel
    valores_invalidos = sorted(serie[mascara_invalida].replace('', '<vazio>').unique().tolist())

    return len(linhas_invalidas) == 0, linhas_invalidas, valores_invalidos

def validar_campo_cnj(df: pd.DataFrame) -> Tuple[bool, List[int]]:
    """Valida se a coluna 'cnj' não começa com espaço em branco."""
    if 'cnj' not in df.columns:
        return False, []

    serie = df['cnj'].astype('string')
    mascara_preenchida = serie.notna()
    mascara_espaco_inicial = serie.str.startswith((' ', '\t'), na=False)
    linhas_invalidas = (df.index[mascara_preenchida & mascara_espaco_inicial] + 2).tolist()
    return len(linhas_invalidas) == 0, linhas_invalidas

def montar_registros(
    df: pd.DataFrame,
    usar_defaults_opcionais: bool = True,
    colunas: Optional[Sequence[str]] = None
) -> List[Tuple]:
    """Transforma o DataFrame em lista de tuplas na mesma ordem de colunas_thproc."""
    colunas_insert = list(colunas or colunas_thproc)
    registros: List[Tuple] = []
    for _, row in df.iterrows():
        valores: List[object] = []
        for coluna in colunas_insert:
            valor = row.get(coluna, None)
            if pd.isna(valor):
                if usar_defaults_opcionais and coluna in OPTIONAL_INPUT_DEFAULTS:
                    valores.append(OPTIONAL_INPUT_DEFAULTS[coluna])
                else:
                    valores.append(None)
            elif isinstance(valor, pd.Timestamp):
                if str(coluna).startswith('data_hora'):
                    valores.append(valor.to_pydatetime())
                elif str(coluna).startswith('data'):
                    valores.append(valor.date())
                else:
                    valores.append(valor)
            else:
                valores.append(valor)
        registros.append(tuple(valores))
    return registros


def verificar_duplicados(cnjs: List[str]) -> List[str]:
    """Verifica quais CNJs já existem no banco e retorna a lista de duplicados."""
    if not cnjs:
        return []

    conn = None
    cur = None

    try:
        conn, cur = conectar_ao_mysql()
        # Usar IN para buscar todos de uma vez
        placeholders = ', '.join(['%s'] * len(cnjs))
        sql = f"SELECT DISTINCT cnj FROM thproc WHERE cnj IN ({placeholders})"
        cur.execute(sql, cnjs)
        duplicados = [row[0] for row in cur.fetchall()]
        return duplicados
    except Exception as e:
        raise RuntimeError(f'Erro ao verificar duplicados:\n{traduzir_erro_mysql(e)}') from e
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def inserir_em_lotes(
    registros: List[Tuple],
    lote: int = 500,
    progress_cb=None,
    colunas: Optional[Sequence[str]] = None
) -> Tuple[int, List[str], Optional[str]]:
    """Executa inserções em lotes na tabela thproc, pulando duplicados.
    Retorna: (total_inserido, lista_cnjs_duplicados, erro)
    """
    conn = None
    cur = None
    cnjs_duplicados: List[str] = []

    try:
        conn, cur = conectar_ao_mysql()
        colunas_insert = list(colunas or colunas_thproc)

        # Extrair todos os CNJs dos registros (cnj é a primeira coluna)
        cnjs_todos = [str(reg[0]).strip() for reg in registros if reg[0] and pd.notna(reg[0])]

        # Verificar duplicados no banco
        cnjs_duplicados_banco = verificar_duplicados(list(dict.fromkeys(cnjs_todos)))
        cnjs_duplicados_set = {str(cnj).strip() for cnj in cnjs_duplicados_banco if cnj}

        # Filtrar registros não duplicados
        registros_limpos = []
        cnjs_vistos_na_carga = set()
        for reg in registros:
            if not reg[0] or pd.isna(reg[0]):
                registros_limpos.append(reg)
                continue

            cnj = str(reg[0]).strip()
            if cnj in cnjs_duplicados_set or cnj in cnjs_vistos_na_carga:
                cnjs_duplicados.append(cnj)
                continue

            cnjs_vistos_na_carga.add(cnj)
            registros_limpos.append(reg)

        if not registros_limpos:
            return 0, cnjs_duplicados, None

        cols = ", ".join(colunas_insert)
        ph = ", ".join(["%s"] * len(colunas_insert))
        sql = f"INSERT INTO thproc ({cols}) VALUES ({ph})"
        total = 0
        for i in range(0, len(registros_limpos), lote):
            chunk = registros_limpos[i:i + lote]
            cur.executemany(sql, chunk)
            conn.commit()
            total += cur.rowcount or 0
            if progress_cb:
                progress_cb(min(total, len(registros_limpos)), len(registros_limpos))
        return total, cnjs_duplicados, None
    except Exception as err:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return 0, cnjs_duplicados, f'Falha ao inserir registros:\n{traduzir_erro_mysql(err)}'
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass
