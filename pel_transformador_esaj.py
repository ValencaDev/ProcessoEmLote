from __future__ import annotations

import re
from typing import Any, Dict, List


class TransformadorESAJ:
    @staticmethod
    def limpar_texto(texto: Any) -> str:
        if texto is None:
            return ""
        texto = re.sub(r"\s+", " ", str(texto))
        return texto.strip()

    @staticmethod
    def normalizar_resultado(resultado: Dict[str, Any]) -> Dict[str, Any]:
        if resultado.get("status") != "sucesso" or not resultado.get("dados"):
            return resultado

        dados = resultado["dados"]
        dados_processo = dados.get("dados_processo", {}) or {}

        dados_processo_normalizado = {
            "classe_judicial": TransformadorESAJ.limpar_texto(dados_processo.get("classe_judicial", "")),
            "assunto": TransformadorESAJ.limpar_texto(dados_processo.get("assunto", "")),
            "jurisdicao": TransformadorESAJ.limpar_texto(dados_processo.get("jurisdicao", "")),
            "orgao_julgador": TransformadorESAJ.limpar_texto(dados_processo.get("orgao_julgador", "")),
            "data_distribuicao": TransformadorESAJ.limpar_texto(dados_processo.get("data_distribuicao", "")),
            "situacao": TransformadorESAJ.limpar_texto(dados_processo.get("situacao", "")),
            "juiz": TransformadorESAJ.limpar_texto(dados_processo.get("juiz", "")),
            "valor_acao": TransformadorESAJ.limpar_texto(
                dados_processo.get("valor_acao", "") or dados_processo.get("valor_causa", "")
            ),
            "area": TransformadorESAJ.limpar_texto(dados_processo.get("area", "Civel")),
            "controle": TransformadorESAJ.limpar_texto(dados_processo.get("controle", "")),
            "outros_numeros": TransformadorESAJ.limpar_texto(dados_processo.get("outros_numeros", "")),
            "outros_assuntos": TransformadorESAJ.limpar_texto(dados_processo.get("outros_assuntos", "")),
        }

        def _normalizar_partes(partes: List[Dict[str, Any]]) -> List[Dict[str, str]]:
            normalizadas: List[Dict[str, str]] = []
            for parte in partes:
                parte_normalizada = {
                    "nome": TransformadorESAJ.limpar_texto(parte.get("nome", "") or parte.get("nome_completo", "")),
                    "nome_completo": TransformadorESAJ.limpar_texto(
                        parte.get("nome_completo", "") or parte.get("nome", "")
                    ),
                    "tipo": TransformadorESAJ.limpar_texto(parte.get("tipo", "")),
                    "situacao": TransformadorESAJ.limpar_texto(parte.get("situacao", "Ativo")),
                    "cpf": TransformadorESAJ.limpar_texto(parte.get("cpf", "")),
                    "cnpj": TransformadorESAJ.limpar_texto(parte.get("cnpj", "")),
                    "oab": TransformadorESAJ.limpar_texto(parte.get("oab", "")),
                    "advogado": TransformadorESAJ.limpar_texto(parte.get("advogado", "")),
                }
                if parte_normalizada["nome"]:
                    normalizadas.append(parte_normalizada)
            return normalizadas

        movimentacoes_normalizadas: List[Dict[str, str]] = []
        for mov in dados.get("movimentacoes", []) or []:
            movimento_texto = mov.get("movimento", "") or mov.get("descricao", "")
            data_mov = mov.get("data", "")
            if not data_mov and movimento_texto:
                data_match = re.search(r"(\d{2}/\d{2}/\d{4})", str(movimento_texto))
                if data_match:
                    data_mov = data_match.group(1)
                    movimento_texto = re.sub(
                        r"^\d{2}/\d{2}/\d{4}\s*[-–]\s*",
                        "",
                        str(movimento_texto),
                    ).strip()

            movimentacao_normalizada = {
                "data": TransformadorESAJ.limpar_texto(data_mov),
                "movimento": TransformadorESAJ.limpar_texto(movimento_texto),
                "descricao": TransformadorESAJ.limpar_texto(movimento_texto),
                "hora": TransformadorESAJ.limpar_texto(mov.get("hora", "")),
                "documento": TransformadorESAJ.limpar_texto(mov.get("documento", "")),
            }
            if movimentacao_normalizada["movimento"]:
                movimentacoes_normalizadas.append(movimentacao_normalizada)

        resultado_normalizado = resultado.copy()
        resultado_normalizado["dados"] = {
            "dados_processo": dados_processo_normalizado,
            "polo_ativo": _normalizar_partes(dados.get("polo_ativo", []) or []),
            "polo_passivo": _normalizar_partes(dados.get("polo_passivo", []) or []),
            "movimentacoes": movimentacoes_normalizadas,
            "documentos": dados.get("documentos", []),
        }
        return resultado_normalizado

    @staticmethod
    def normalizar_lote(resultados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        resultados_normalizados: List[Dict[str, Any]] = []
        for resultado in resultados:
            if resultado.get("tribunal") == "8.26":
                resultados_normalizados.append(TransformadorESAJ.normalizar_resultado(resultado))
            else:
                resultados_normalizados.append(resultado)
        return resultados_normalizados
