from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from pel_config import colunas_thproc
from pel_transformador_esaj import TransformadorESAJ


class TransformadorDadosPublicos:
    TRIBUNAL_ESTADO_MAP = {
        "8.19": "RJ",  # TJRJ
        "8.06": "CE",  # TJCE
        "8.13": "MG",  # TJMG
        "8.17": "PE",  # TJPE
        "8.26": "SP",  # eSAJ SP
        "8.10": "MA",  # TJMA
        "8.07": "DF",  # TJDFT
        "8.20": "RN",  # TJRN
    }

    TRIBUNAL_NOME_MAP = {
        "8.19": "TJRJ",
        "8.06": "TJCE",
        "8.13": "TJMG",
        "8.17": "TJPE",
        "8.26": "eSAJ SP",
        "8.10": "TJMA",
        "8.07": "TJDFT",
        "8.20": "TJRN",
    }

    @staticmethod
    def limpar_texto(texto: Any) -> str:
        if texto is None:
            return ""
        texto = re.sub(r"\s+", " ", str(texto))
        return texto.strip()

    @staticmethod
    def normalizar_nome_participante(nome: str) -> str:
        if not nome:
            return ""
        nome = TransformadorDadosPublicos.limpar_texto(nome)
        nome = re.sub(
            r"\s*registrado\(a\)\s+civilmente\s+como\s+.+$",
            "",
            nome,
            flags=re.IGNORECASE,
        )
        return nome.strip()

    @staticmethod
    def extrair_tipo_rito(classe_judicial: str) -> str:
        if not classe_judicial:
            return "Nao informado"
        classe_lower = classe_judicial.lower()
        if "juizado especial" in classe_lower or "sumar" in classe_lower or "sumarissimo" in classe_lower:
            return "Sumarissimo"
        if "ordinario" in classe_lower:
            return "Ordinario"
        if "especial" in classe_lower:
            return "Especial"
        return "Nao informado"

    @staticmethod
    def obter_estado_por_tribunal(tribunal_key: str) -> str:
        return TransformadorDadosPublicos.TRIBUNAL_ESTADO_MAP.get(tribunal_key, "")

    @staticmethod
    def obter_sistema_externo(tribunal_key: str) -> str:
        nome_tribunal = TransformadorDadosPublicos.TRIBUNAL_NOME_MAP.get(tribunal_key, "")
        estado = TransformadorDadosPublicos.obter_estado_por_tribunal(tribunal_key)
        if tribunal_key == "8.26":
            return f"TJ-{estado}-eSAJ-1 grau" if estado else ""
        if nome_tribunal and estado:
            return f"TJ-{estado}-PJE-1 grau"
        return ""

    @staticmethod
    def extrair_unidade_especialidade(classe_judicial: str, orgao_julgador: str = "") -> Tuple[str, str]:
        unidade = ""
        especialidade = "Civel"
        classe_lower = TransformadorDadosPublicos.limpar_texto(classe_judicial).lower()
        orgao_lower = TransformadorDadosPublicos.limpar_texto(orgao_julgador).lower()

        if "juizado especial" in classe_lower:
            unidade = "Juizado Especial"
        elif "vara" in classe_lower:
            unidade = "Vara"
        elif "juizado" in classe_lower:
            unidade = "Juizado"

        if not unidade:
            if "juizado" in orgao_lower:
                unidade = "Juizado Especial"
            elif "vara" in orgao_lower:
                unidade = "Vara"
            elif "nucleo" in orgao_lower or "nucleo" in orgao_lower:
                unidade = "Nucleo"

        return unidade, especialidade

    @staticmethod
    def extrair_numero_unidade(orgao_julgador: str) -> str:
        orgao_limpo = TransformadorDadosPublicos.limpar_texto(orgao_julgador)
        if not orgao_limpo:
            return "0"
        match = re.search(r"(\d+)[ºª]", orgao_limpo)
        if match:
            return match.group(1)
        return "0"

    @staticmethod
    def extrair_comarca_estado(jurisdicao: str, orgao_julgador: str = "") -> Tuple[str, str]:
        comarca = ""
        jurisdicao_limpa = TransformadorDadosPublicos.limpar_texto(jurisdicao)
        orgao_limpo = TransformadorDadosPublicos.limpar_texto(orgao_julgador)

        if orgao_limpo:
            match_orgao = re.search(r"comarca de (.+)", orgao_limpo, re.IGNORECASE)
            if match_orgao:
                comarca = match_orgao.group(1).strip().rstrip(".")

        if not comarca and jurisdicao_limpa:
            match = re.search(r"comarca de (.+)", jurisdicao_limpa, re.IGNORECASE)
            if match:
                comarca = match.group(1).strip()
            elif "comarca" in jurisdicao_limpa.lower():
                match2 = re.search(r"comarca[:\s]+(.+)", jurisdicao_limpa, re.IGNORECASE)
                if match2:
                    comarca = match2.group(1).strip()
            else:
                comarca = jurisdicao_limpa

        if not comarca:
            comarca = orgao_limpo or jurisdicao_limpa

        return comarca, ""

    @staticmethod
    def processar_movimentacoes(movimentacoes: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        eventos: List[Dict[str, str]] = []
        for mov in movimentacoes:
            movimento = mov.get("movimento", "") or mov.get("descricao", "")
            movimento = TransformadorDadosPublicos.limpar_texto(movimento)
            if not movimento:
                continue

            data_evento = TransformadorDadosPublicos.limpar_texto(mov.get("data", ""))
            hora_evento = TransformadorDadosPublicos.limpar_texto(mov.get("hora", ""))
            if not data_evento:
                data_match = re.search(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}:\d{2}))?", movimento)
                if data_match:
                    data_evento = data_match.group(1)
                    if not hora_evento and data_match.group(2):
                        hora_evento = data_match.group(2)

            complemento = TransformadorDadosPublicos.limpar_texto(mov.get("documento", ""))
            if hora_evento:
                complemento = f"{complemento} {hora_evento}".strip()

            eventos.append(
                {
                    "dataEvento": data_evento,
                    "tipoEvento": "1035",
                    "descricaoEvento": movimento,
                    "complementoEvento": complemento,
                    "observacaoEvento": "",
                    "solicitanteEvento": "",
                    "responsavelEvento": "",
                }
            )
        return eventos

    @staticmethod
    def _base_registro() -> Dict[str, Any]:
        registro = {col: None for col in colunas_thproc}
        registro.update(
            {
                "natureza": "Judicial",
                "materia": "Civel",
                "tipoInstancia": "1a Instancia",
                "processoEletronico": "Sim",
                "processoEstrategico": "Nao",
                "status": "Ativo",
                "grupoProcesso": "101",
                "prioridadeDe": "438",
                "corresponsavel": "32",
                "dataNotificacao": "Agora",
                "probabilidadePerda": "Possivel",
                "tipoAndamento": "Nao informado",
                "solicitanteAndamento": "438",
                "responsavelAndamento": "438",
                "corresponsavelAndamento": "438",
                "descricaoObjeto": "Nao informado",
                "tipoAcao": "Reclamacao",
            }
        )
        return registro

    @staticmethod
    def transformar_dados(resultado: Dict[str, Any]) -> Dict[str, Any]:
        if resultado.get("status") != "sucesso" or not resultado.get("dados"):
            return {}
        if resultado.get("tribunal") == "8.26":
            resultado = TransformadorESAJ.normalizar_resultado(resultado)

        dados = resultado.get("dados", {})
        dados_processo = dados.get("dados_processo", {}) or {}
        polo_ativo = dados.get("polo_ativo", []) or []
        polo_passivo = dados.get("polo_passivo", []) or []
        movimentacoes = dados.get("movimentacoes", []) or []

        numero_processo = TransformadorDadosPublicos.limpar_texto(resultado.get("numero_processo", ""))
        tribunal_key = TransformadorDadosPublicos.limpar_texto(resultado.get("tribunal", ""))
        classe_judicial = TransformadorDadosPublicos.limpar_texto(dados_processo.get("classe_judicial", ""))
        jurisdicao = TransformadorDadosPublicos.limpar_texto(dados_processo.get("jurisdicao", ""))
        orgao_julgador = TransformadorDadosPublicos.limpar_texto(dados_processo.get("orgao_julgador", ""))
        data_distribuicao = TransformadorDadosPublicos.limpar_texto(dados_processo.get("data_distribuicao", ""))
        valor_causa = TransformadorDadosPublicos.limpar_texto(
            dados_processo.get("valor_acao", "") or dados_processo.get("valor_causa", "")
        )

        unidade, especialidade = TransformadorDadosPublicos.extrair_unidade_especialidade(
            classe_judicial, orgao_julgador
        )
        comarca, _ = TransformadorDadosPublicos.extrair_comarca_estado(jurisdicao, orgao_julgador)
        tipo_rito = TransformadorDadosPublicos.extrair_tipo_rito(classe_judicial)
        numero_unidade = TransformadorDadosPublicos.extrair_numero_unidade(orgao_julgador)
        estado = TransformadorDadosPublicos.obter_estado_por_tribunal(tribunal_key)
        sistema_externo = TransformadorDadosPublicos.obter_sistema_externo(tribunal_key)

        partes_ativas_nomes: List[str] = []
        advogados_polo_ativo: List[str] = []
        for parte in polo_ativo:
            nome = TransformadorDadosPublicos.limpar_texto(parte.get("nome", "") or parte.get("nome_completo", ""))
            nome_completo = TransformadorDadosPublicos.limpar_texto(parte.get("nome_completo", ""))
            tipo = TransformadorDadosPublicos.limpar_texto(parte.get("tipo", ""))
            if not nome:
                continue
            nome_exibicao = TransformadorDadosPublicos.normalizar_nome_participante(nome)
            tipo_upper = tipo.upper()
            nome_completo_upper = nome_completo.upper()
            nome_upper = nome.upper()
            is_advogado = (
                "ADVOGADO" in tipo_upper
                or "ADVOGADO" in nome_completo_upper
                or "ADVOGADO" in nome_upper
                or "OAB" in nome_upper
            )
            if not nome_exibicao:
                continue
            if is_advogado:
                oab = TransformadorDadosPublicos.limpar_texto(parte.get("oab", ""))
                advogados_polo_ativo.append(f"{nome_exibicao} - OAB {oab}" if oab else nome_exibicao)
            else:
                partes_ativas_nomes.append(nome_exibicao)

        partes_passivas_nomes: List[str] = []
        for parte in polo_passivo:
            nome = TransformadorDadosPublicos.limpar_texto(parte.get("nome", "") or parte.get("nome_completo", ""))
            nome_exibicao = TransformadorDadosPublicos.normalizar_nome_participante(nome)
            if nome_exibicao:
                partes_passivas_nomes.append(nome_exibicao)

        eventos = TransformadorDadosPublicos.processar_movimentacoes(movimentacoes)
        data_status = ""
        if movimentacoes:
            primeira_mov = movimentacoes[0]
            data_status = TransformadorDadosPublicos.limpar_texto(primeira_mov.get("data", ""))
            if not data_status:
                movimento = primeira_mov.get("movimento", "") or primeira_mov.get("descricao", "")
                data_match = re.search(r"(\d{2}/\d{2}/\d{4})", str(movimento))
                if data_match:
                    data_status = data_match.group(1)

        registro = TransformadorDadosPublicos._base_registro()
        registro.update(
            {
                "cnj": numero_processo,
                "numeroProcessoCNJ": numero_processo,
                "tipoPartePoloAtivo": "Autor" if partes_ativas_nomes else "",
                "partePoloAtivo": "; ".join(partes_ativas_nomes),
                "Adv_parte_contraria": "; ".join(advogados_polo_ativo),
                "tipoPartePoloPassivo": "Reu" if partes_passivas_nomes else "",
                "partePoloPassivo": "; ".join(partes_passivas_nomes),
                "cliente": partes_passivas_nomes[0] if partes_passivas_nomes else "",
                "tipoDeRito": tipo_rito,
                "dataDistribuicao": data_distribuicao,
                "numeroUnidade": numero_unidade,
                "unidade": unidade,
                "especialidade": especialidade,
                "comarca": comarca,
                "estado": estado,
                "orgao": orgao_julgador,
                "sistemaExterno": sistema_externo,
                "valorCausa": valor_causa,
                "valorFinalCausa": valor_causa,
                "dataStatus": data_status,
            }
        )

        if eventos:
            evento = eventos[0]
            registro.update(evento)

        return registro

    @staticmethod
    def transformar_lote(resultados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        resultados = TransformadorESAJ.normalizar_lote(resultados)
        saida: List[Dict[str, Any]] = []
        for resultado in resultados:
            transformado = TransformadorDadosPublicos.transformar_dados(resultado)
            if transformado:
                saida.append(transformado)
        return saida


def transformar_lote_publico(resultados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return TransformadorDadosPublicos.transformar_lote(resultados)
