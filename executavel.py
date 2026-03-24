from __future__ import annotations
import os
import re
import socket
import threading
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import pandas as pd
from dotenv import load_dotenv
import mysql.connector

# ==============================
# Config & Constantes
# ==============================

from pathlib import Path
import sys

LAST_ENV_PATH = None  # para debug

def carregar_variaveis_ambiente():
    """Carrega variáveis do .env em cenários normais e empacotados (PyInstaller).

    Estratégia:
      1) Tenta o diretório de trabalho atual sem sobrescrever variáveis já definidas.
      2) Se estiver empacotado (sys.frozen), tenta o diretório do executável e o _MEIPASS;
         caso contrário, tenta o diretório do arquivo atual (__file__).
      3) Por fim, tenta novamente o CWD com override=True.
      Atualiza LAST_ENV_PATH com o caminho utilizado.
    """
    global LAST_ENV_PATH
    load_dotenv(override=False)  # passo 1

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

    candidates.append(Path.cwd() / '.env')  # passo 3

    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=p, override=True)
            LAST_ENV_PATH = str(p)
            break

def obter_config_banco() -> dict:
    """Lê credenciais/ajustes do .env (carregado por carregar_variaveis_ambiente)."""
    carregar_variaveis_ambiente()
    cfg = {
        'host'    : os.getenv('DB_HOST', ''),
        'user'    : os.getenv('DB_USER', ''),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_DATABASE', ''),
        'port'    : int(os.getenv('DB_PORT', '3306')),
        'timeout' : int(os.getenv('DB_TIMEOUT', '15')),
    }
    # opcionais
    auth_plugin = os.getenv('DB_AUTH_PLUGIN', '').strip()   # ex: mysql_native_password
    if auth_plugin:
        cfg['auth_plugin'] = auth_plugin
    ssl_ca   = os.getenv('DB_SSL_CA', '').strip()
    ssl_cert = os.getenv('DB_SSL_CERT', '').strip()
    ssl_key  = os.getenv('DB_SSL_KEY', '').strip()
    if ssl_ca:
        cfg['ssl_ca'] = ssl_ca
    if ssl_cert:
        cfg['ssl_cert'] = ssl_cert
    if ssl_key:
        cfg['ssl_key'] = ssl_key
    return cfg

# Colunas esperadas em thproc (na ordem exata para o INSERT)
colunas_thproc = [
    'cnj', 'tipoPartePoloAtivo', 'partePoloAtivo', 'tipoPartePoloPassivo',
    'partePoloPassivo', 'cliente', 'tipoDeRito', 'dataDistribuicao',
    'numeroUnidade', 'unidade', 'especialidade', 'comarca', 'estado',
    'orgao', 'natureza', 'materia', 'dataInstancia', 'tipoInstancia',
    'valorCausa', 'tipoAcao', 'tipoObjeto', 'dataFase', 'fase', 'dataStatus',
    'status', 'carteira', 'prioridadeDe', 'dataEvento', 'tipoEvento',
    'descricaoEvento', 'solicitanteEvento', 'responsavelEvento',
    'corresponsavel', 'teste', 'pasta', 'numeroProcessoAnterior',
    'numeroProcessoCNJ', 'sistemaExterno', 'processoEletronico',
    'processoEstrategico', 'grupoProcesso', 'complementoEvento',
    'observacaoEvento', 'grupoTrabalho', 'dataNotificacao',
    'dataNotificacaoAdicional', 'probabilidadePerda',
    'dataValorProvisionado', 'valorProvisionado', 'dataAndamento',
    'tipoAndamento', 'descricaoAndamento', 'complementoAndamento',
    'solicitanteAndamento', 'responsavelAndamento', 'corresponsavelAndamento',
    'descricaoObjeto', 'escritorioCredenciado', 'data_hora_verificacao',
    'usuario_verificado_id', 'verificado', 'nomegrupo_id', 'cliente_id',
    'exportado', 'dataContratacao', 'observacaoDoProcesso',
    'parecerDoProcesso', 'data_hora_submit', 'data_hora_export',
    'usuario_submit_id', 'usuario_export_id', 'valorFinalCausa',
    'tipoPoloCliente', 'data_resultado', 'tipo_resultado',
    'descricao_resultado', 'Adv_parte_contraria', 'codnatureza',
    'codparte_polo_ativo', 'codpolo_cliente', 'codsistema_externo',
    'codstatus', 'codfase', 'codespecialidade', 'codorgao', 'codmateria',
    'codtipo_rito', 'codcomarca', 'codparte_polo_passivo', 'codunidade',
    'codtipo_instancia', 'codcorresponsavelAndamento', 'codlote'
]

colunas_thproc_regular = colunas_thproc + ['excluido']

# Mapeamento de renome da planilha -> colunas do banco
RENAME_MAP = {
    'Pasta': 'pasta',
    'Nº do Processo Anterior': 'numeroProcessoAnterior',
    'Nº do Processo CNJ': 'cnj',
    'Tipo de Parte Pólo Ativo': 'tipoPartePoloAtivo',
    'Parte Pólo Ativo': 'partePoloAtivo',
    'Tipo de Parte Pólo Passivo': 'tipoPartePoloPassivo',
    'Parte Pólo Passivo': 'partePoloPassivo',
    'Cliente': 'cliente',
    'Tipo de Rito': 'tipoDeRito',
    'Data de Distribuição': 'dataDistribuicao',
    'Número Unidade': 'numeroUnidade',
    'Unidade': 'unidade',
    'Especialidade': 'especialidade',
    'Comarca': 'comarca',
    'Estado': 'estado',
    'Órgão': 'orgao',
    'Natureza': 'natureza',
    'Matéria': 'materia',
    'Data da Instância': 'dataInstancia',
    'Tipo de Instância': 'tipoInstancia',
    'Sistema Externo': 'sistemaExterno',
    'Processo Eletrônico': 'processoEletronico',
    'Processo Estratégico': 'processoEstrategico',
    'Valor da Causa': 'valorCausa',
    'Valor Final da Causa': 'valorFinalCausa',
    'Tipo de Ação': 'tipoAcao',
    'Tipo de Objeto': 'tipoObjeto',
    'Data da Fase': 'dataFase',
    'Fase': 'fase',
    'Data do Status': 'dataStatus',
    'Status': 'status',
    'Grupo de Processo': 'grupoProcesso',
    'Prioridade De': 'prioridadeDe',
    'Data do Resultado': 'data_resultado',
    'Tipo de Resultado': 'tipo_resultado',
    'Descrição do Resultado': 'descricao_resultado',
    'Data Evento': 'dataEvento',
    'Tipo Evento': 'tipoEvento',
    'Descrição Evento': 'descricaoEvento',
    'Complemento Evento': 'complementoEvento',
    'Observação Evento': 'observacaoEvento',
    'Solicitante Evento': 'solicitanteEvento',
    'Responsável Evento': 'responsavelEvento',
    'Grupo de Trabalho': 'grupoTrabalho',
    'Corresponsável': 'corresponsavel',
    'Data de Notificação': 'dataNotificacao',
    'Data de Notificação Adicional': 'dataNotificacaoAdicional',
    'Probabilidade de Perda': 'probabilidadePerda',
    'Data do valor provisionado': 'dataValorProvisionado',
    'Valor Provisionado': 'valorProvisionado',
    'Data do Andamento': 'dataAndamento',
    'Tipo do Andamento': 'tipoAndamento',
    'Descrição do Andamento': 'descricaoAndamento',
    'Complemento do Andamento': 'complementoAndamento',
    'Solicitante do Andamento': 'solicitanteAndamento',
    'Responsável do Andamento': 'responsavelAndamento',
    'Corresponsável do Andamento': 'corresponsavelAndamento',
    'Descrição do objeto': 'descricaoObjeto',
    'Escritório Credenciado': 'escritorioCredenciado',
    'Data da Contratação': 'dataContratacao',
    'Observação do Processo': 'observacaoDoProcesso',
    'Parecer do Processo': 'parecerDoProcesso',
}

# Presets por empresa
TODAY_STR = datetime.now().strftime('%d/%m/%Y')
EMPRESA_REGULAR = 'Direito Tributario'
COMPANY_PRESETS: Dict[str, Dict[str, object]] = {
    EMPRESA_REGULAR: {},
    'ENEL (RJ)': {
        'nomegrupo_id': '101',
        'solicitanteAndamento': '356',
        'responsavelAndamento': '353',
        'corresponsavelAndamento': '438',
        'prioridadeDe': '356',
        'orgao': 'TJ - RJ',
        'tipoEvento': '1035',
        'corresponsavel': '32',
        'solicitanteEvento': '356',
        'responsavelEvento': '353',
        'codlote': f'Enel {TODAY_STR}',
        'carteira': '101',
    },
    'STONE': {
        'nomegrupo_id': '49',
        'prioridadeDe': '102',
        'solicitanteAndamento': '102',
        'responsavelAndamento': '102',
        'corresponsavelAndamento': '102',
        'orgao': 'STJ',
        'corresponsavel': '30',
        'codlote': f'Stone {TODAY_STR}',
        'carteira': '49',
        'responsavelEvento': '102',
        'tipoEvento': '979',
        'solicitanteEvento': '102',
    },
    'CAGECE': {
        'nomegrupo_id': '6',
        'prioridadeDe': '135',
        'solicitanteAndamento': '135',
        'responsavelAndamento': '135',
        'corresponsavelAndamento': '135',
        'orgao': 'TJ - CE',
        'corresponsavel': '30',
        'codlote': f'Cagece {TODAY_STR}',
        'carteira': '6',
        'tipoEvento': '979',
        'responsavelEvento': '135',
        'solicitanteEvento': '135',
        'tipoAndamento': 'Não Informado',
    },
    'NOTREDAME – CÍVEL': {
        'nomegrupo_id': '21',
        'prioridadeDe': '431',
        'solicitanteAndamento': '99',
        'responsavelAndamento': '99',
        'corresponsavelAndamento': '99',
        'orgao': 'TJ - SP',
        'corresponsavel': '6',
        'codlote': f'NOTREDAME-CIVEL {TODAY_STR}',
        'carteira': '21',
        'tipoEvento': '380',
        'solicitanteEvento': '99',
        'tipoAndamento': 'Não Informado',
        'descricaoObjeto': 'Aviso prévio da rescisão posterior a 12 meses',
        'tipoObjeto': 'Aviso Prévio/Multa',
        'responsavelEvento': '99',
    },
    'NOTREDAME – ESTRATÉGICO': {
        'nomegrupo_id': '52',
        'prioridadeDe': '481',
        'solicitanteAndamento': '481',
        'responsavelAndamento': '481',
        'corresponsavelAndamento': '481',
        'orgao': 'TJ - SP',
        'corresponsavel': '33',
        'codlote': f'NOTREDAME-ESTRATEGICO {TODAY_STR}',
        'carteira': '52',
        'tipoEvento': '1049',
        'solicitanteEvento': '481',
        'tipoAndamento': 'Não Informado',
        'descricaoObjeto': 'Não Informado',
        'tipoObjeto': 'Aviso Prévio/Multa',
        'responsavelEvento': '481',
    },
    'NOTREDAME – TRABALHISTA': {
        'nomegrupo_id': '21',
        'prioridadeDe': '477',
        'solicitanteAndamento': '477',
        'responsavelAndamento': '477',
        'corresponsavelAndamento': '477',
        'orgao': 'TRT - 22º REGIAO',
        'corresponsavel': '31',
        'codlote': f'NOTREDAME-TRABALISTA {TODAY_STR}',
        'carteira': '48',
        'tipoEvento': '1002',
        'solicitanteEvento': '477',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '477',

    },
    'PORTO SEGURO': {
        'nomegrupo_id': '27',
        'prioridadeDe': '124',
        'solicitanteAndamento': '124',
        'responsavelAndamento': '124',
        'corresponsavelAndamento': '124',
        'orgao': 'TJ - SP',
        'corresponsavel': '7',
        'codlote': f'PORTOSEGURO {TODAY_STR}',
        'carteira': '27',
        'tipoEvento': '431',
        'solicitanteEvento': '124',
        'tipoAndamento': 'Não Informado',
        'descricaoObjeto': 'Aviso prévio da rescisão posterior a 12 meses',
        'tipoObjeto': 'Aviso Prévio/Multa',
        'responsavelEvento': '124',
    },
    'MOVIDA - PASSIVO ':{
        'nomegrupo_id': '62',
        'prioridadeDe':'341',
        'solicitanteAndamento': '341',
        'responsavelAndamento': '341',
        'corresponsavelAndamento': '341',
        'orgao': 'STJ',
        'corresponsavel': '5',
        'codlote': f'MOVIDA {TODAY_STR}',
        'carteira': '62',
        'tipoEvento': '1201',
        'solicitanteEvento': '341',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '341',
    },
    'ANCAR':{
        'nomegrupo_id': '3',
        'prioridadeDe':'140',
        'solicitanteAndamento': '140',
        'responsavelAndamento': '140',
        'corresponsavelAndamento': '140',
        'orgao': 'STJ',
        'corresponsavel': '2',
        'codlote': f'ANCAR {TODAY_STR}',
        'carteira': '3',
        'tipoEvento': '129',
        'solicitanteEvento': '140',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '140',
    },
    'PAGUE MENOS CIVEL':{
        'nomegrupo_id': '200',
        'prioridadeDe':'8',
        'solicitanteAndamento': '61',
        'responsavelAndamento': '61',
        'corresponsavelAndamento': '61',
        'orgao': 'STJ',
        'corresponsavel': '9',
        'codlote': f'PAGUE MENOS CIVEL {TODAY_STR}',
        'carteira': '29',
        'tipoEvento': '999',
        'solicitanteEvento': '61',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '8',

    },
    'PAGUE MENOS TRABALHISTA':{
        'nomegrupo_id': '200',
        'prioridadeDe':'479',
        'solicitanteAndamento': '227',
        'responsavelAndamento': '227',
        'corresponsavelAndamento': '227',
        'orgao': 'STJ',
        'corresponsavel': '15',
        'codlote': f'PAGUE MENOS TRABALHISTA {TODAY_STR}',
        'carteira': '35',
        'tipoEvento': '462',
        'solicitanteEvento': '227',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '479',
    },
    'AMBEV - CIVEL':{
        'nomegrupo_id': '1',
        'prioridadeDe':'63',
        'solicitanteAndamento': '63',
        'responsavelAndamento': '63',
        'corresponsavelAndamento': '63',
        'orgao': 'STJ',
        'corresponsavel': '1',
        'codlote': f'AMBEV - CIVEL {TODAY_STR}',
        'carteira': '1',
        'tipoEvento': '1',
        'solicitanteEvento': '63',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '63',
  },
    'RAIA DROGASIL ':{
        'nomegrupo_id': '59',
        'prioridadeDe':'63',
        'solicitanteAndamento': '63',
        'responsavelAndamento': '63',
        'corresponsavelAndamento': '63',
        'orgao': 'STJ',
        'corresponsavel': '42',
        'codlote': f'RAIA DROGASIL {TODAY_STR}',
        'carteira': '59',
        'tipoEvento': '1',
        'solicitanteEvento': '63',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '63',
  },
    'UNIP - CIVEL ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'180',
        'solicitanteAndamento': '457',
        'responsavelAndamento': '457',
        'corresponsavelAndamento': '457',
        'orgao': 'STJ',
        'corresponsavel': '19',
        'codlote': f'UNIP- CIVEL {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1010',
        'solicitanteEvento': '457',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '180',
  },
    'UNIP - CIVEL - ADM ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'180',
        'solicitanteAndamento': '457',
        'responsavelAndamento': '457',
        'corresponsavelAndamento': '457',
        'corresponsavel': '19',
        'codlote': f'UNIP- CIVEL - ADM {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1027',
        'solicitanteEvento': '457',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '180',
  },
    'UNIP - TRABALHISTA ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'81',
        'solicitanteAndamento': '81',
        'responsavelAndamento': '81',
        'corresponsavelAndamento': '81',
        'orgao': 'TST',
        'corresponsavel': '19',
        'codlote': f'UNIP- TRABALHISTA {TODAY_STR}',
        'carteira': '119',
        'tipoEvento': '1032',
        'solicitanteEvento': '81',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '81',
  },
    'DIGITAL COLLEGE ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'285',
        'solicitanteAndamento': '285',
        'responsavelAndamento': '285',
        'corresponsavelAndamento': '285',
        'orgao': 'STJ',
        'corresponsavel': '19',
        'codlote': f' DIGITAL COLLEGE {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1010',
        'solicitanteEvento': '',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '285',
},
    'FGV ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'285',
        'solicitanteAndamento': '285',
        'responsavelAndamento': '285',
        'corresponsavelAndamento': '285',
        'orgao': 'STJ',
        'corresponsavel': '19',
        'codlote': f' FGV {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1010',
        'solicitanteEvento': '',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '285',
  },
    'GRAU TECNICO ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'285',
        'solicitanteAndamento': '285',
        'responsavelAndamento': '285',
        'corresponsavelAndamento': '285',
        'orgao': 'STJ',
        'corresponsavel': '19',
        'codlote': f' GRAU TECNICO {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1010',
        'solicitanteEvento': '',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '285',
  },
    'UNIGRANDE ':{
        'nomegrupo_id': '39',
        'prioridadeDe':'180',
        'solicitanteAndamento': '457',
        'responsavelAndamento': '457',
        'corresponsavelAndamento': '457',
        'orgao': 'STJ',
        'corresponsavel': '19',
        'codlote': f'UNIGRANDE {TODAY_STR}',
        'carteira': '39',
        'tipoEvento': '1010',
        'solicitanteEvento': '457',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '180',
  },
    'TRABALHISTA':{
        'nomegrupo_id': '40',
        'prioridadeDe':'483',
        'solicitanteAndamento': '438',
        'responsavelAndamento': '438',
        'corresponsavelAndamento': '438',
        'orgao': 'STJ',
        'corresponsavel': '23',
        'codlote': f'TRABALHISTA {TODAY_STR}',
        'carteira': '40',
        'tipoEvento': '590',
        'solicitanteEvento': '438',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '492',
    },
    'ITAPEVA':{
        'nomegrupo_id': '65',
        'prioridadeDe':'513',
        'solicitanteAndamento': '513',
        'responsavelAndamento': '56',
        'corresponsavelAndamento': '513',
        'orgao': 'STJ',
        'corresponsavel': '47',
        'codlote': f'ITAPEVA {TODAY_STR}',
        'carteira': '65',
        'tipoEvento': '1234',
        'solicitanteEvento': '513',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '513',
    },
    'SERVIÇOS':{
        'nomegrupo_id': '32',
        'prioridadeDe':'88',
        'solicitanteAndamento': '202',
        'responsavelAndamento': '202',
        'corresponsavelAndamento': '202',
        'orgao': 'STJ',
        'corresponsavel': '12',
        'codlote': f'SERVIÇOS {TODAY_STR}',
        'carteira': '32',
        'tipoEvento': '1238',
        'solicitanteEvento': '202',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '88',
    },
    'PUBLICO':{
        'nomegrupo_id': '55',
        'prioridadeDe':'201',
        'solicitanteAndamento': '201',
        'responsavelAndamento': '201',
        'corresponsavelAndamento': '201',
        'corresponsavel': '37',
        'codlote': f'PUBLICO {TODAY_STR}',
        'carteira': '55',
        'tipoEvento': '1084',
        'solicitanteEvento': '201',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '255',
    },
    'MOVIDA PASSIVO - ADM':{
        'nomegrupo_id': '62',
        'prioridadeDe':'519',
        'solicitanteAndamento': '19',
        'responsavelAndamento': '19',
        'corresponsavelAndamento': '19',
        'corresponsavel': '43',
        'codlote': f'MOVIDA PASSIVO - ADM {TODAY_STR}',
        'carteira': '62',
        'tipoEvento': '1211',
        'solicitanteEvento': '19',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '519',
    },
    'ORIGINAL PASSIVO':{
        'nomegrupo_id': '60',
        'prioridadeDe':'4',
        'solicitanteAndamento': '71',
        'responsavelAndamento': '71',
        'corresponsavelAndamento': '71',
        'corresponsavel': '44',
        'codlote': f'ORIGINAL PASSIVO {TODAY_STR}',
        'carteira': '60',
        'tipoEvento': '1192',
        'solicitanteEvento': '71',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '4',
    },
    'AMBEV - TRAB':{
        'nomegrupo_id': '1',
        'prioridadeDe':'386',
        'solicitanteAndamento': '227',
        'responsavelAndamento': '227',
        'corresponsavelAndamento': '227',
        'corresponsavel': '18',
        'codlote': f'AMBEV - TRAB {TODAY_STR}',
        'carteira': '36',
        'tipoEvento': '39',
        'solicitanteEvento': '227',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '386',
     },
    'COBRANÇA JUDICIAL':{
        'nomegrupo_id': '1',
        'prioridadeDe':'557',
        'solicitanteAndamento': '245',
        'responsavelAndamento': '245',
        'corresponsavelAndamento': '245',
        'corresponsavel': '18',
        'codlote': f'COBRANÇA JUDICIAL {TODAY_STR}',
        'carteira': '41',
        'tipoEvento': '39',
        'solicitanteEvento': '245',
        'tipoAndamento': 'Não Informado',
        'responsavelEvento': '557',
    },
}

COMMON_DEFAULTS = {
    'tipoPoloCliente': 'Passivo',
    'codpolo_cliente': '2',
    'dataNotificacao': 'Agora',
    'dataNotificacaoAdicional': 'No dia do Evento',
    'escritorioCredenciado': 'VALENÇA & ASSOCIADOS',
    'usuario_submit_id': '420',
    'verificado': 0,
    'exportado': 0,
}

# Colunas da planilha que podem faltar sem bloquear a importacao.
OPTIONAL_INPUT_DEFAULTS = {
    'numeroUnidade': None,
    'unidade': '',
    'especialidade': '',
    'comarca': '',
    'estado': '',
    'materia': '',
    'tipoInstancia': '',
}

OPTIONAL_INPUT_COLUMNS = set(OPTIONAL_INPUT_DEFAULTS)

# Campos de texto que limitaremos a 255 caracteres
CAMPOS_TEXTO_255 = [
    'partePoloAtivo','partePoloPassivo','cliente','tipoDeRito','unidade','especialidade',
    'comarca','estado','orgao','natureza','materia','tipoInstancia','tipoAcao','tipoObjeto',
    'fase','status','carteira','prioridadeDe','tipoEvento','solicitanteEvento','responsavelEvento',
    'corresponsavel','sistemaExterno','tipoAndamento','solicitanteAndamento','responsavelAndamento',
    'corresponsavelAndamento','descricaoObjeto','escritorioCredenciado','tipoPoloCliente','tipo_resultado',
    'Adv_parte_contraria'
]

COLUNAS_DATA = [
    'dataDistribuicao','dataInstancia','dataFase','dataStatus','dataEvento','dataValorProvisionado',
    'dataAndamento','dataContratacao','data_resultado'
]

# ==============================
# Funções utilitárias
# ==============================

def teste_tcp(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, str]:
    """Tenta abrir um socket TCP; retorna (ok, erro_str). Ajuda a diagnosticar erro 2003."""
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




# ==============================
# UI Tkinter
# ==============================

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
        ttk.Button(btns, text='Janela Alternativa', command=self.open_alt_window).pack(side='left', padx=6)

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

# ==============================
# Main
# ==============================

if __name__ == '__main__':
    app = MigracoesApp()
    app.mainloop()
