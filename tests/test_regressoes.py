import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

import cadastroCliente as cadastro
import executavel
import executavelnewversion as novo
import pel_db
import pel_importacao
from pel_validacao import converter_datas, converter_moeda


class ValidacaoTests(unittest.TestCase):
    def test_moeda_brasileira_e_numerica(self):
        valores = converter_moeda(pd.Series(['R$ 1.234,56', '12,50', 10.25, '10.25', '-1,20', None, '']), 'valorCausa')
        self.assertEqual(valores.tolist(), [Decimal('1234.56'), Decimal('12.50'), Decimal('10.25'), Decimal('10.25'), Decimal('-1.20'), None, None])

    def test_moeda_invalida_informa_linha(self):
        for valor in ('abc', '1.234', '1,234.56', '1.2.3,45', float('inf')):
            with self.subTest(valor=valor), self.assertRaisesRegex(ValueError, 'valorCausa.*linhas 4'):
                converter_moeda(pd.Series([valor], index=[2]), 'valorCausa')

    def test_datas_preservam_vazios_e_formatos(self):
        valores = converter_datas(pd.Series(['05/06/2024', '2024-06-05', None, '']), 'dataEvento')
        self.assertEqual(valores.tolist(), [date(2024, 6, 5), date(2024, 6, 5), None, None])

    def test_data_invalida_nao_vira_hoje(self):
        for valor in ('31/02/2024', 'invalida', 45678):
            with self.subTest(valor=valor), self.assertRaisesRegex(ValueError, 'dataEvento.*linhas 2'):
                converter_datas(pd.Series([valor]), 'dataEvento')

    def test_horario_submit_preservado(self):
        valor = converter_datas(pd.Series(['2024-06-05 14:35:20']), 'data_hora_submit', manter_hora=True).iloc[0]
        self.assertEqual(valor, datetime(2024, 6, 5, 14, 35, 20))

    def test_todos_os_fluxos_usam_conversao_segura(self):
        for modulo in (executavel, pel_db, pel_importacao):
            with self.subTest(modulo=modulo.__name__):
                df = modulo.formatar_datas_e_numeros(pd.DataFrame({'valorCausa': ['1.234,56'], 'dataEvento': [None]}))
                self.assertEqual(df['valorCausa'].iloc[0], Decimal('1234.56'))
                self.assertIsNone(df['dataEvento'].iloc[0])
                with self.assertRaises(ValueError):
                    modulo.formatar_datas_e_numeros(pd.DataFrame({'dataEvento': ['invalida']}))

    def test_novo_fluxo_bloqueia_data_invalida(self):
        with self.assertRaises(ValueError):
            novo.formatar_datas_para_envio(pd.DataFrame({'dataEvento': ['invalida']}))
        df = novo.formatar_datas_para_envio(pd.DataFrame({'dataEvento': ['2024-06-05', None]}))
        self.assertEqual(df['dataEvento'].tolist(), ['2024-06-05', None])


class ClienteTests(unittest.TestCase):
    CNPJ = '11.222.333/0001-81'

    def test_digitos_verificadores(self):
        self.assertTrue(cadastro.validar_cnpj(self.CNPJ))
        for valor in ('11.222.333/0001-82', '00000000000000', 'abc', '123'):
            self.assertFalse(cadastro.validar_cnpj(valor))

    def test_documentos_invalidos_nao_acessam_banco(self):
        with patch.object(cadastro, 'conectar_ao_mysql') as conectar:
            for valor in ('abc', '123', '11.222.333/0001-82', ''):
                self.assertFalse(cadastro.atualizar_nome_cliente(valor, 'Nome')[0])
                self.assertFalse(cadastro.cadastrar_cliente('Equipe', 'Grupo', 'Nome', valor)[0])
            conectar.assert_not_called()

    def test_atualizacao_usa_documento_exato(self):
        conn, cur = MagicMock(), MagicMock()
        cur.rowcount = 1
        with patch.object(cadastro, 'conectar_ao_mysql', return_value=(conn, cur, None)):
            self.assertTrue(cadastro.atualizar_nome_cliente(self.CNPJ, 'Cliente')[0])
        sql, parametros = cur.execute.call_args.args
        self.assertNotIn('LIKE', sql)
        self.assertIn('= %s', sql)
        self.assertEqual(parametros, ('CLIENTE', '11222333000181'))
        conn.commit.assert_called_once()

    def test_grupo_usa_id_gerado_pelo_banco(self):
        cur = MagicMock()
        cur.fetchone.return_value = ('id', 'int', 'NO', 'PRI', None, 'auto_increment')
        cur.lastrowid = 85
        self.assertEqual(cadastro.inserir_grupo(cur, 5, 'Grupo'), 85)
        sql, parametros = cur.execute.call_args.args
        self.assertIn('(codequipe, nomegrupo)', sql)
        self.assertEqual(parametros, (5, 'Grupo'))

    def test_grupo_sem_auto_increment_bloqueia_insert(self):
        cur = MagicMock()
        cur.fetchone.return_value = ('id', 'int', 'NO', 'PRI', None, '')
        with self.assertRaisesRegex(RuntimeError, 'AUTO_INCREMENT'):
            cadastro.inserir_grupo(cur, 5, 'Grupo')
        self.assertEqual(cur.execute.call_count, 1)


class TransacaoTests(unittest.TestCase):
    def executar(self, modulo, falha=False):
        conn, cur = MagicMock(), MagicMock()
        cur.rowcount = 1
        cur.executemany.side_effect = [None, RuntimeError('falha no segundo lote')] if falha else None
        with patch.object(modulo, 'conectar_ao_mysql', return_value=(conn, cur)):
            if modulo is novo:
                resultado = novo.inserir_em_lotes_sem_tratamento(pd.DataFrame({'cnj': ['1', '2']}), ['cnj'], lote=1)
            else:
                with patch.object(modulo, 'verificar_duplicados', return_value=[]):
                    resultado = modulo.inserir_em_lotes([('1',), ('2',)], lote=1, colunas=['cnj'])
        cur.close.assert_called_once()
        conn.close.assert_called_once()
        return conn, resultado

    def test_falha_segundo_lote_reverte_tudo(self):
        for modulo in (executavel, pel_db, novo):
            with self.subTest(modulo=modulo.__name__):
                conn, resultado = self.executar(modulo, falha=True)
                conn.commit.assert_not_called()
                conn.rollback.assert_called_once()
                self.assertEqual(resultado[0], 0)
                self.assertIsNotNone(resultado[-1])

    def test_sucesso_confirma_uma_unica_vez(self):
        for modulo in (executavel, pel_db, novo):
            with self.subTest(modulo=modulo.__name__):
                conn, resultado = self.executar(modulo)
                conn.commit.assert_called_once()
                conn.rollback.assert_not_called()
                self.assertEqual(resultado[0], 2)
                self.assertIsNone(resultado[-1])


class EmpacotamentoTests(unittest.TestCase):
    def test_specs_nao_embutem_env(self):
        raiz = Path(__file__).resolve().parents[1]
        for spec in raiz.glob('*.spec'):
            with self.subTest(spec=spec.name):
                self.assertNotIn("('.env', '.')", spec.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
