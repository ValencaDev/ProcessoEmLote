from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from pel_config import (
    CAMPOS_TEXTO_255,
    COLUNAS_DATA,
    COMMON_DEFAULTS,
    COMPANY_PRESETS,
    EMPRESA_REGULAR,
    OPTIONAL_INPUT_COLUMNS,
    OPTIONAL_INPUT_DEFAULTS,
    colunas_thproc,
)
from pel_transformador_eproc_sp import dataframe_de_arquivo_eproc_sp
from pel_transformador_publico import transformar_lote_publico

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


def dataframe_de_resultados_publicos(resultados: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    """
    Converte payload de tribunais de acesso publico para DataFrame no schema de thproc.
    """
    registros = transformar_lote_publico(list(resultados))
    if not registros:
        return pd.DataFrame(columns=colunas_thproc)

    df = pd.DataFrame(registros)
    for col in colunas_thproc:
        if col not in df.columns:
            df[col] = None
    return df[colunas_thproc]


def dataframe_de_excel_eproc_sp(caminho: str) -> pd.DataFrame:
    return dataframe_de_arquivo_eproc_sp(caminho)
