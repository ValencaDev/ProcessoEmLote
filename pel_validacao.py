"""Conversões de planilha que preservam vazios e rejeitam dados inválidos."""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re

import pandas as pd


def _vazio(valor):
    return pd.isna(valor) or (isinstance(valor, str) and not valor.strip())


def _converter_serie(serie, coluna, conversor):
    valores, erros = [], []
    for posicao, (indice, valor) in enumerate(serie.items(), start=2):
        try:
            valores.append(None if _vazio(valor) else conversor(valor))
        except (ValueError, TypeError, InvalidOperation, OverflowError):
            linha = int(indice) + 2 if isinstance(indice, int) else posicao
            erros.append(str(linha))
            valores.append(None)
    if erros:
        raise ValueError(f"Coluna '{coluna}': valores inválidos nas linhas {', '.join(erros)}. Corrija a planilha antes de enviar.")
    return pd.Series(valores, index=serie.index, dtype=object)


def _decimal(valor):
    if isinstance(valor, bool):
        raise ValueError('Valor monetário inválido')
    if isinstance(valor, str):
        valor = valor.strip()
        if valor.startswith('R$'):
            valor = valor[2:].strip()
        if ',' in valor:
            if not re.fullmatch(r'[+-]?(?:\d+|\d{1,3}(?:\.\d{3})+),\d{1,2}', valor):
                raise ValueError('Formato monetário inválido')
            valor = valor.replace('.', '').replace(',', '.')
        elif not re.fullmatch(r'[+-]?\d+(?:\.\d{1,2})?', valor):
            # Pontos de milhar só são aceitos com a vírgula decimal explícita.
            raise ValueError('Formato monetário ambíguo')
    numero = Decimal(str(valor))
    if not numero.is_finite():
        raise ValueError('Valor não finito')
    return numero


def converter_moeda(serie, coluna):
    return _converter_serie(serie, coluna, _decimal)


def _data(valor):
    if isinstance(valor, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(valor)
    if not isinstance(valor, str):
        raise ValueError('Informe uma data, não um número')
    texto = valor.strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}(?:$|[ T])', texto):
        return pd.Timestamp(datetime.fromisoformat(texto))
    return pd.to_datetime(texto, dayfirst=True, errors='raise')


def converter_datas(serie, coluna, manter_hora=False):
    valores = _converter_serie(serie, coluna, _data)
    return valores.map(lambda valor: None if valor is None else (valor.to_pydatetime() if manter_hora else valor.date()))
