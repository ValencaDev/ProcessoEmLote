import queue
import tempfile
import threading
import unittest
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

import executavel as app


# Numeração fictícia com DV calculado pelo Módulo 97 Base 10.
CNJ = '0000100-80.2008.8.15.0001'


def interface(caminho='planilha.xlsx'):
    tela = SimpleNamespace(
        state=app.AppState(path=caminho), _busy=False, _sending=False,
        _closing=False, _events=queue.Queue(), lbl_status={},
        alt_lbl_status={}, alt_pb=MagicMock(), pb=MagicMock(),
        tree=MagicMock(), btn_send=MagicMock(), btn_file=MagicMock(),
        btn_preview=MagicMock(), btn_test=MagicMock(), ent_path=MagicMock(),
        cmb_sheet=MagicMock(), cmb_empresa=MagicMock(), cmb_natureza=MagicMock(),
        after=MagicMock(), report_callback_exception=MagicMock(), destroy=MagicMock(),
    )
    tela.tree.get_children.return_value = ()
    tela.cmb_sheet.get.return_value = 'Dados'
    tela.cmb_empresa.get.return_value = 'STONE'
    tela.cmb_natureza.get.return_value = 'Judicial'
    for nome in (
        '_set_busy', '_invalidate_preview', '_context_key', '_natureza_selecionada',
        '_run_background', '_drain_events', '_on_close', 'on_select_file',
        'on_preview', 'on_send', 'on_send_importacao', '_start_send',
        'on_test_conn', '_update_progress',
    ):
        setattr(tela, nome, MethodType(getattr(app.MigracoesApp, nome), tela))
    tela._render_preview = MagicMock()
    tela._finish_send = MagicMock()
    tela._alt_finish_send = MagicMock()
    tela._alt_update_progress = MagicMock()
    return tela


class CNJTests(unittest.TestCase):
    def test_cnj_com_e_sem_mascara(self):
        self.assertTrue(app.cnj_judicial_valido(CNJ))
        self.assertTrue(app.cnj_judicial_valido('00001008020088150001'))

    def test_vazios_formato_e_digitos_invalidos(self):
        for valor in (None, pd.NA, '', 'abc', '0000100-81.2008.8.15.0001', ' ' + CNJ, 1.23e19):
            with self.subTest(valor=valor):
                self.assertFalse(app.cnj_judicial_valido(valor))

    def test_administrativa_aceita_identificador_livre_mas_nao_vazio(self):
        df = pd.DataFrame({'cnj': ['ADM-123', ''], 'natureza': ['Administrativa'] * 2})
        self.assertEqual(app.validar_campo_cnj(df), (False, [3]))

    def test_fluxo_regular_tambem_bloqueia_cnj_invalido(self):
        with patch.object(app.pd, 'read_excel', return_value=pd.DataFrame({'cnj': ['abc']})):
            with self.assertRaisesRegex(ValueError, 'CNJ inválido'):
                app.preparar_previa('planilha.xlsx', 'Dados', app.EMPRESA_REGULAR, 'Judicial')

    def test_fluxo_regular_aceita_cnj_valido(self):
        with patch.object(app.pd, 'read_excel', return_value=pd.DataFrame({'cnj': [CNJ]})):
            df, faltando = app.preparar_previa('planilha.xlsx', 'Dados', app.EMPRESA_REGULAR, 'Judicial')
        self.assertEqual(df['cnj'].iloc[0], CNJ)
        self.assertEqual(faltando, [])


class InterfaceTests(unittest.TestCase):
    def setUp(self):
        self.tela = interface()
        self.workers = []
        self.patch_thread = patch.object(app.threading, 'Thread', side_effect=self.thread)
        self.patch_thread.start()
        self.addCleanup(self.patch_thread.stop)
        for nome in ('showerror', 'showwarning', 'showinfo'):
            patcher = patch.object(app.messagebox, nome)
            patcher.start()
            self.addCleanup(patcher.stop)

    def thread(self, **kwargs):
        return SimpleNamespace(start=lambda: self.workers.append(kwargs['target']))

    def executar_worker(self):
        self.workers.pop(0)()
        self.tela._drain_events()
        self.tela.report_callback_exception.assert_not_called()

    def preparar_envio(self):
        self.tela.state.df = pd.DataFrame({'cnj': [CNJ], 'natureza': ['Judicial']})
        self.tela.state.empresa = 'STONE'
        self.tela.state.preview_key = self.tela._context_key()

    def test_troca_de_opcoes_bloqueia_previa_antiga(self):
        for combo, valor in (('cmb_empresa', 'ENEL (RJ)'), ('cmb_sheet', 'Outra'), ('cmb_natureza', 'Administrativa')):
            with self.subTest(combo=combo):
                self.preparar_envio()
                widget = getattr(self.tela, combo)
                anterior = widget.get.return_value
                widget.get.return_value = valor
                self.tela.on_send()
                self.assertIsNone(self.tela.state.df)
                self.assertFalse(self.workers)
                widget.get.return_value = anterior

    def test_alteracao_do_arquivo_bloqueia_envio(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / 'dados.xlsx'
            arquivo.write_bytes(b'antes')
            self.tela.state.path = str(arquivo)
            self.preparar_envio()
            arquivo.write_bytes(b'depois-com-outro-tamanho')
            self.tela.on_send()
            self.assertIsNone(self.tela.state.df)
            self.assertFalse(self.workers)

    def test_preview_com_erro_limpa_dados_e_desbloqueia_interface(self):
        self.preparar_envio()
        self.tela.on_preview()
        self.assertIsNone(self.tela.state.df)
        self.assertTrue(self.tela._busy)
        with patch.object(app, 'preparar_previa', side_effect=ValueError('erro de validação')):
            self.executar_worker()
        self.assertIsNone(self.tela.state.df)
        self.assertFalse(self.tela._busy)
        app.messagebox.showerror.assert_called_once_with('Pré-visualização bloqueada', 'erro de validação')

    def test_resultado_de_preview_obsoleto_e_descartado(self):
        self.tela.on_preview()
        self.tela.cmb_empresa.get.return_value = 'ENEL (RJ)'
        with patch.object(app, 'preparar_previa', return_value=(pd.DataFrame({'cnj': [CNJ]}), [])):
            self.executar_worker()
        self.assertIsNone(self.tela.state.df)
        self.assertFalse(self.tela._busy)

    def test_dois_cliques_iniciam_um_envio_e_bloqueiam_fechamento(self):
        self.preparar_envio()
        self.tela.on_send()
        self.tela.on_send()
        self.tela._on_close()
        self.assertEqual(len(self.workers), 1)
        self.assertTrue(self.tela._sending)
        self.tela.destroy.assert_not_called()
        with patch.object(app, 'montar_registros', return_value=[(CNJ,)]), patch.object(app, 'inserir_em_lotes', return_value=(1, [], None)):
            self.executar_worker()
        self.assertFalse(self.tela._busy)
        self.assertFalse(self.tela._sending)
        self.tela._finish_send.assert_called_once()

    def test_envio_revalida_antes_de_acessar_banco(self):
        self.preparar_envio()
        self.tela.state.df.loc[0, 'cnj'] = 'abc'
        self.tela.on_send()
        with patch.object(app, 'inserir_em_lotes') as inserir:
            self.executar_worker()
            inserir.assert_not_called()
        self.assertFalse(self.tela._sending)
        erro = self.tela._finish_send.call_args.args[-1]
        self.assertIn('CNJ inválido', erro)

    def test_erro_de_conexao_sobrevive_ao_callback(self):
        self.tela.on_test_conn()
        with patch.object(app, 'conectar_ao_mysql', side_effect=RuntimeError('erro original')):
            self.executar_worker()
        app.messagebox.showerror.assert_called_once_with('Erro MySQL', 'erro original')
        self.assertFalse(self.tela._busy)

    def test_consulta_e_fechamento_da_conexao_acontecem_no_worker(self):
        conn, cur = MagicMock(), MagicMock()
        self.tela.on_test_conn()
        cur.execute.assert_not_called()
        with patch.object(app, 'conectar_ao_mysql', return_value=(conn, cur)):
            self.executar_worker()
        cur.execute.assert_called_once_with('SELECT 1')
        cur.close.assert_called_once()
        conn.close.assert_called_once()


class BackgroundTests(unittest.TestCase):
    def test_worker_real_entrega_resultado_sem_acessar_tkinter(self):
        tela = interface()
        ids = []
        concluido = threading.Event()
        finish = MagicMock()
        def work():
            ids.append(threading.get_ident())
            concluido.set()
            return 'pronto'
        tela._run_background(work, finish, MagicMock())
        self.assertTrue(concluido.wait(2))
        callback, args = tela._events.get(timeout=2)
        self.assertNotEqual(ids[0], threading.get_ident())
        finish.assert_not_called()
        tela.after.assert_not_called()
        callback(*args)
        finish.assert_called_once_with('pronto')


if __name__ == '__main__':
    unittest.main()
