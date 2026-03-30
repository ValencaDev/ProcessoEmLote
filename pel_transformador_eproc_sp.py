from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from pel_config import colunas_thproc


def limpar_nome_remover_cnpj(texto: Any) -> str:

    if texto is None or (isinstance(texto, float) and pd.isna(texto)):
        return ""
    texto = str(texto).strip()
    if not texto:
        return ""

    if "; " in texto:
        nomes_limpos = []
        for nome in texto.split("; "):
            nome_limpo = nome.strip()
            if "(" in nome_limpo:
                nome_limpo = nome_limpo.split("(")[0].strip()
            nomes_limpos.append(nome_limpo)
        return "; ".join(nomes_limpos)

    if "(" in texto:
        return texto.split("(")[0].strip()
    return texto


def formatar_data_apenas_data(data_completa: Any) -> str:
    if data_completa is None or (isinstance(data_completa, float) and pd.isna(data_completa)):
        return ""
    data_completa = str(data_completa).strip()
    if not data_completa:
        return ""
    if " " in data_completa:
        return data_completa.split(" ", 1)[0]
    return data_completa


def _base_registro_vazio() -> Dict[str, Any]:
    return {col: None for col in colunas_thproc}


def linha_eproc_sp_para_registro_thproc(row: pd.Series, agora: Optional[datetime] = None) -> Dict[str, Any]:
    agora = agora or datetime.now()

    polo_passivo_raw = row.get("Polo Passivo", "")
    polo_passivo = limpar_nome_remover_cnpj(polo_passivo_raw)
    data_autuacao = str(row.get("Data de Autuação", "") or "").strip()
    situacao = str(row.get("Situação", "") or "").strip()

    if situacao and situacao.upper() != "NÃO ENCONTRADO":
        status = "Ativo"
    else:
        status = situacao or ""

    reg = _base_registro_vazio()
    reg.update(
        {
            "pasta": "",
            "numeroProcessoAnterior": "",
            "cnj": str(row.get("Número do Processo", "") or "").strip(),
            "numeroProcessoCNJ": str(row.get("Número do Processo", "") or "").strip(),
            "tipoPartePoloAtivo": "Autor",
            "partePoloAtivo": str(row.get("Polo Ativo", "") or "").strip(),
            "tipoPartePoloPassivo": "Réu",
            "partePoloPassivo": polo_passivo,
            "cliente": polo_passivo,
            "tipoDeRito": "Sumarissimo",
            "dataDistribuicao": formatar_data_apenas_data(data_autuacao),
            "numeroUnidade": None,
            "unidade": "Juizado Especial",
            "especialidade": "Cível",
            "comarca": str(row.get("Órgão Julgador", "") or "").strip(),
            "estado": "SP",
            "orgao": "TJ-SP",
            "natureza": "Judicial",
            "materia": "Cível",
            "dataInstancia": formatar_data_apenas_data(data_autuacao),
            "tipoInstancia": "1ª Instância",
            "sistemaExterno": "EPROC-SP",
            "processoEletronico": "Sim",
            "processoEstrategico": "Não",
            "valorCausa": str(row.get("Valor da Causa", "") or "").strip(),
            "valorFinalCausa": "",
            "tipoAcao": "Reclamação do Consumidor",
            "tipoObjeto": "",
            "dataFase": "",
            "fase": "",
            "dataStatus": formatar_data_apenas_data(data_autuacao),
            "status": status,
            "grupoProcesso": "",
            "prioridadeDe": "",
            "dataEvento": "",
            "tipoEvento": "1035",
            "descricaoEvento": "",
            "complementoEvento": "",
            "observacaoEvento": "",
            "solicitanteEvento": "",
            "responsavelEvento": "",
            "corresponsavel": "32",
            "dataNotificacao": agora.strftime("%d/%m/%Y %H:%M:%S"),
            "dataNotificacaoAdicional": "No dia do Evento",
            "probabilidadePerda": "Possível",
            "dataValorProvisionado": str(row.get("Data/Hora da Consulta", "") or "").strip(),
            "valorProvisionado": "",
            "dataAndamento": "",
            "tipoAndamento": "Não informado",
            "descricaoAndamento": "",
            "complementoAndamento": "",
            "solicitanteAndamento": "",
            "responsavelAndamento": "",
            "corresponsavelAndamento": "",
            "descricaoObjeto": "Não Informado",
            "escritorioCredenciado": "VALENÇA & ASSOCIADOS",
            "dataContratacao": "",
            "observacaoDoProcesso": "",
            "parecerDoProcesso": "",
            "data_resultado": "",
            "tipo_resultado": "",
            "descricao_resultado": "",
            "grupoTrabalho": "",
        }
    )
    return reg


def dataframe_de_planilha_eproc_sp(df: pd.DataFrame, agora: Optional[datetime] = None) -> pd.DataFrame:
    agora = agora or datetime.now()
    registros = [linha_eproc_sp_para_registro_thproc(row, agora=agora) for _, row in df.iterrows()]
    if not registros:
        return pd.DataFrame(columns=colunas_thproc)

    out = pd.DataFrame(registros)
    for col in colunas_thproc:
        if col not in out.columns:
            out[col] = None
    return out[colunas_thproc]


def ler_excel_eproc_sp(caminho: Union[str, Path]) -> pd.DataFrame:
    return pd.read_excel(Path(caminho))


def dataframe_de_arquivo_eproc_sp(caminho: Union[str, Path], agora: Optional[datetime] = None) -> pd.DataFrame:
    df = ler_excel_eproc_sp(caminho)
    return dataframe_de_planilha_eproc_sp(df, agora=agora)


def salvar_tratado_eproc_sp(
    caminho_entrada: Union[str, Path],
    caminho_saida: Optional[Union[str, Path]] = None,
    agora: Optional[datetime] = None,
) -> Path:

    caminho_entrada = Path(caminho_entrada)
    if caminho_saida is None:
        ts = (agora or datetime.now()).strftime("%Y%m%d_%H%M%S")
        caminho_saida = caminho_entrada.parent / f"processos_tratados_{ts}.xlsx"
    else:
        caminho_saida = Path(caminho_saida)

    df = dataframe_de_arquivo_eproc_sp(caminho_entrada, agora=agora)
    df.to_excel(caminho_saida, index=False, engine="openpyxl")
    return caminho_saida
