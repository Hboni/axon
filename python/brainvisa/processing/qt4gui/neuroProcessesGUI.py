# -*- coding: utf-8 -*-
#  This software and supporting documentation are distributed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
# This software is governed by the CeCILL license version 2 under
# French law and abiding by the rules of distribution of free software.
# You can  use, modify and/or redistribute the software under the
# terms of the CeCILL license version 2 as circulated by CEA, CNRS
# and INRIA at the following URL "http://www.cecill.info".
#
# As a counterpart to the access to the source code and  rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty  and the software's author,  the holder of the
# economic rights,  and the successive licensors  have only  limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading,  using,  modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean  that it is complicated to manipulate,  and  that  also
# therefore means  that it is reserved for developers  and  experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and,  more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license version 2 and that you accept its terms.

from __future__ import print_function
import brainvisa.processes
from datetime import date
from datetime import datetime
from datetime import timedelta
from six.moves import StringIO
import distutils
import os
import sys
import re
import types
from brainvisa.processing.qtgui.backwardCompatibleQt import *
from soma.qt_gui import qt_backend
from soma.qt_gui.qt_backend.QtGui import QKeySequence
from soma.qt_gui.qt_backend import QtCore
from soma.qt_gui.qt_backend import QtGui
try:
    from soma.qt_gui.qt_backend import QtWebEngine
    from soma.qt_gui.qt_backend.QtWebEngineWidgets import QWebEnginePage
    use_webengine = True
except ImportError:
    from soma.qt_gui.qt_backend import QtWebKit
    QWebEnginePage = QtWebKit.QWebPage
    use_webengine = False
from soma.qt_gui.qt_backend import loadUi, loadUiType
from brainvisa.configuration import neuroConfig
from brainvisa.configuration.qt4gui import neuroConfigGUI
from brainvisa.processing.qt4gui import neuroLogGUI
from brainvisa.data import neuroData
from brainvisa.history import ProcessExecutionEvent
from brainvisa.data.neuroDiskItems import DiskItem
from brainvisa.data.readdiskitem import ReadDiskItem
from brainvisa.data.writediskitem import WriteDiskItem
from brainvisa.data.qt4gui import lockFilesGUI, neuroDataGUI
import weakref
from soma.minf.xhtml import XHTML
from soma.qtgui.api import QtThreadCall, FakeQtThreadCall, WebBrowserWithSearch, bigIconSize, defaultIconSize
from soma.html import htmlEscape
from soma.wip.application.api import Application
import soma.functiontools
import threading
import socket
import six
try:
    import sip
except:
    # for sip 3.x (does it work ??)
    import libsip as sip
# for comatibility, make mainThreadActions visible in neuroProcessesGUI
from brainvisa.processes import mainThreadActions

from brainvisa.processing import neuroException
from soma.qtgui.api import EditableTreeWidget, TreeListWidget
from soma.notification import ObservableList, EditableTree
from soma.signature.api import HasSignature
from soma.signature.api import Signature as SomaSignature
from soma.signature.api import FileName as SomaFileName
from soma.signature.api import Choice as SomaChoice
from soma.signature.api import Boolean as SomaBoolean
from soma.qt4gui.api import ApplicationQt4GUI
from brainvisa.data.databaseCheck import BVChecker_3_1
from brainvisa.data import neuroHierarchy
from brainvisa.tools import checkbrainvisaupdates
try:
    from six.moves.urllib import parse as urllib
except ImportError:
    # some six versions do not provide six.moves.urllib (Ubuntu 12.04)
    import urllib

# Because soma_workflow uses soma.qt_gui.qt_backend.uic, we need to first
# initialize
# soma.qt_gui.qt_backend.uic in order to hack soma.qt_gui.qt_backend.uic and
# fix issue #13432
# ref: https://bioproj.extra.cea.fr/redmine/issues/13432
import soma.qt_gui.qt_backend.uic

try:
    from soma_workflow.gui.workflowGui import SomaWorkflowWidget as ComputingResourceWidget
    from soma_workflow.gui.workflowGui import SomaWorkflowMiniWidget as MiniComputingResourceWidget
    import soma_workflow.gui.workflowGui
    from soma_workflow.gui.workflowGui import ComputingResourcePool
    from soma_workflow.gui.workflowGui import ApplicationModel as WorkflowApplicationModel
    import soma_workflow.configuration
except ImportError:
    _soma_workflow = False

    class ComputingResourceWidget(object):
        pass

    class MiniComputingResourceWidget(object):
        pass

    class ComputingResourcePool(object):
        pass

    class WorkflowApplicationModel(object):
        pass
else:
    _soma_workflow = True

if sys.version_info[0] >= 3:
    def items(thing):
        return list(thing.items())
    unicode = str
    xrange = range
else:
    def items(thing):
        return thing.items()

#----------------------------------------------------------------------------


def restartAnatomist():
    from brainvisa import anatomist
    a = anatomist.Anatomist(create=False)
    if hasattr(a, '_restartshell_launched'):
        a.launched = True
        del a._restartshell_launched


class _ProcDeleter(object):

    def __init__(self, o):
        self.o = o

    def __del__(self):
        try:
            self.o.kill()
        except:
            pass


def startShell():
    from soma.qt_gui.qt_backend.QtGui import qApp
    qt_impl = qt_backend.get_qt_backend()
    if qt_impl == 'PyQt4':
        os.environ[
            "QT_API"] = "pyqt"  # prevent ipython from trying to use PySide
    elif qt_impl == 'PyQt5':
        os.environ["QT_API"] = "pyqt5"
    elif qt_impl == 'PySide':
        os.environ["QT_API"] = "pyside"

    try:
        # try using jupyter qtconsole
        import jupyter_core
        ipConsole = brainvisa.processes.runIPConsoleKernel()
        print('ipConsole:', ipConsole, file=sys.stderr)
        sys.stderr.flush()
        import soma.subprocess
        python = 'python%d' % sys.version_info[0]
        sp = soma.subprocess.Popen([
            sys.executable, '-c',
            'import os; os.environ["QT_API"] = "%s"; '
            'from jupyter_core.command import main; main()'
            % os.environ["QT_API"],
            'qtconsole', '--kernel', python, '--existing',
            '--shell=%d' % ipConsole.shell_port,
            '--iopub=%d' % ipConsole.iopub_port,
            '--stdin=%d' % ipConsole.stdin_port,
            '--hb=%d' % ipConsole.hb_port])
        brainvisa.processes._ipsubprocs.append(_ProcDeleter(sp))

        return
    except:
        pass

    try:
        import IPython
        ipversion = [int(x) for x in IPython.__version__.split('.')]
        if ipversion >= [0, 11]:
            # ipython >= 0.11, use client/server mode
            ipConsole = brainvisa.processes.runIPConsoleKernel()
            import soma.subprocess
            if ipversion >= [1, 0]:
                ipmodule = 'IPython.terminal.ipapp'
            else:
                ipmodule = 'IPython.frontend.terminal.ipapp'
            sp = soma.subprocess.Popen([
                sys.executable, '-c',
                'import os; os.environ["QT_API"] = "%s"; from %s import launch_new_instance; launch_new_instance()'
                % (os.environ["QT_API"], ipmodule),
                'qtconsole', '--existing',
                '--shell=%d' % ipConsole.shell_port,
                '--iopub=%d' % ipConsole.iopub_port,
                '--stdin=%d' % ipConsole.stdin_port,
                '--hb=%d' % ipConsole.hb_port])
            brainvisa.processes._ipsubprocs.append(_ProcDeleter(sp))
            return
    except:
        pass
    neuroConfig.shell = True
    try:
        if neuroConfig.anatomistImplementation == 'socket':
            from brainvisa import anatomist
            a = anatomist.Anatomist(create=False)
            if a and a.launched:
                a.launched = False
                a._restartshell_launched = True
    except Exception as e:
        print(e)
    mainThreadActions().push(qApp.exit)


#----------------------------------------------------------------------------
def quitRequest():
    # Called when quitting brainvisa using quit menu or closing the main window
    # print('!!!!!!!!!quitRequest!!!!!!!!')
    a = QMessageBox.warning(
        None, _t_('Quit'), _t_('Do you really want to quit BrainVISA ?'),
                            QMessageBox.Yes | QMessageBox.Default, QMessageBox.No)
    if a == QMessageBox.Yes:
        wids = qApp.topLevelWidgets()
        for w in wids:
            if isinstance(w, ProcessView) or isinstance(w, SomaWorkflowProcessView):
                w.close()
                del w
        try:
            from brainvisa import anatomist
            a = anatomist.Anatomist(create=False)
            if a:
                close_viewers()
                a.close()
        except:
            pass
        if neuroConfig.shell:
            sys.exit()
        else:
            qApp.exit()

#----------------------------------------------------------------------------


def cleanupGui():
    # called when quitting a ipython shell
    wids = qApp.topLevelWidgets()
    for w in wids:
        if isinstance(w, ProcessView):
            w.close()
            del w
    wids = qApp.topLevelWidgets()
    for w in wids:
        w.close()
        del w

#----------------------------------------------------------------------------
_helpWidget = None


def helpRequest():
    url = QUrl.fromLocalFile(neuroConfig.getDocFile(
        os.path.join('help', 'index.html'))).toString()
    openWeb(url)


def runHtmlBrowser(source, existingWidget=None):
    ''' run the HTML browser defined in BV config. If it is a builtin browser,
    use an existing widget, or instantiate and return a new one.
    '''
    try:
        browser = neuroConfig.HTMLBrowser
        if browser is not None:
            browser = distutils.spawn.find_executable(browser)
            if browser:
                if sys.platform == "darwin":
                    m = re.match(
                        "\/Applications\/.+\.app/Contents/MacOS/(.*)", browser)
                    if m:
                        if os.system("open -a " + m.group(1) + " '" + source + "'") == 0:
                            return

                env = os.environ.copy()
                if (not browser.startswith(os.path.dirname(neuroConfig.mainPath))):  # external command
                    if neuroConfig.brainvisaSysEnv:
                        env.update(neuroConfig.brainvisaSysEnv.getVariables())
                if os.spawnle(os.P_NOWAIT, browser, browser, source, env) > 0:
                    return
    except:
        pass
    if existingWidget is None:
        existingWidget = HTMLBrowser(None)
        existingWidget.setWindowTitle(_t_('BrainVISA help'))
        existingWidget.resize(800, 600)
    sys.stdout.flush()
    existingWidget.setSource(source)
    existingWidget.show()
    existingWidget.raise_()
    return existingWidget


def openWeb(source):
    global _helpWidget
    widget = runHtmlBrowser(source, _helpWidget)
    if widget is not None:
        _helpWidget = widget

#----------------------------------------------------------------------------


def runCsvViewer(source, existingWidget=None):
    ''' run the CSV viewer defined in BV config. If it is a builtin viewer,
    use an existing widget, or instantiate and return a new one.
    '''
    try:
        configuration = Application().configuration
        browser = configuration.brainvisa.csvViewer
        if browser is not None:
            browser = distutils.spawn.find_executable(browser)
            if browser:
                if sys.platform == "darwin":
                    m = re.match(
                        "\/Applications\/.+\.app/Contents/MacOS/(.*)", browser)
                    if m:
                        if os.system("open -a " + m.group(1) + " '" + source + "'") == 0:
                            return

                env = os.environ.copy()
                if (not browser.startswith(os.path.dirname(neuroConfig.mainPath))):  # external command
                    if neuroConfig.brainvisaSysEnv:
                        env.update(neuroConfig.brainvisaSysEnv.getVariables())
                if os.spawnle(os.P_NOWAIT, browser, browser, source, env) > 0:
                    return
    except Exception as e:
        print('exception.')
        print(e)
        pass

    # builtin browser, needs GenericTableEditor
    try:
        from soma.qt_gui.generic_table_editor import GenericTableEditor
        if existingWidget is None:
            existingWidget = GenericTableEditor(None)
            existingWidget.setWindowTitle(_t_('CSV viewer'))
            existingWidget.resize(800, 600)
        existingWidget.load_from_file(source)
        existingWidget.show()
        existingWidget.raise_()
        return existingWidget
    except:
        pass
    # fallback to text editor
    textEditor = configuration.brainvisa.textEditor
    if textEditor is not None:
        textEditor = distutils.spawn.find_executable(textEditor)
        if textEditor:
            env = os.environ.copy()
            if (not textEditor.startswith(os.path.dirname(neuroConfig.mainPath))):  # external command
                if neuroConfig.brainvisaSysEnv:
                    env.update(neuroConfig.brainvisaSysEnv.getVariables())
            if os.spawnle(os.P_NOWAIT, textEditor, textEditor, source, env) != 0:
                return
    raise RuntimeError(_t_('Could not run a CSV viewer program.'))


#----------------------------------------------------------------------------
class SomaWorkflowMiniWidget(MiniComputingResourceWidget):

    def __init__(self, model, sw_widget, parent=None):
        super(SomaWorkflowMiniWidget, self).__init__(model,
                                                     sw_widget,
                                                     parent)
        sw_widget.update_workflow_list_from_model = True
        sw_widget.hide()


class SomaWorkflowWidget(ComputingResourceWidget):

    # dict wf_id -> serialized_process
    serialized_processes = None

    brainvisa_code = "brainvisa_"

    def __init__(self, model, computing_resource=None, parent=None):
        super(SomaWorkflowWidget, self).__init__(model,
                                                 None,
                                                 False,
                                                 computing_resource,
                                                 parent,
                                                 0)

        self.ui.list_widget_submitted_wfs.itemDoubleClicked.connect(
            self.workflow_double_clicked)
        self.ui.resource_selection_frame.hide()

    def workflow_filter(self, workflows):
        new_workflows = {}
        self.serialized_processes = {}
        for wf_id, (name, date) in six.iteritems(workflows):
            if name != None and \
                len(name) > len(SomaWorkflowWidget.brainvisa_code) - 1 and \
              name[0:len(SomaWorkflowWidget.brainvisa_code)] == SomaWorkflowWidget.brainvisa_code:
                new_workflows[wf_id] = (
                    name[len(SomaWorkflowWidget.brainvisa_code):], date)
        return new_workflows

    @QtCore.Slot()
    def workflow_double_clicked(self):
        selected_items = self.ui.list_widget_submitted_wfs.selectedItems()
        wf_id = int(selected_items[0].data(QtCore.Qt.UserRole))

        if wf_id not in self.serialized_processes:
            workflow = self.model.current_workflow(
            )  # current_connection.workflow(wf_id)
            if workflow == None:
                QMessageBox.warning(
                    self, "Workflow loading impossible", "The workflow does not exist.")
                return
            workflow = workflow.server_workflow
            if workflow.user_storage != None and \
                len(workflow.user_storage) == 2 and \
                    workflow.user_storage[0] == SomaWorkflowWidget.brainvisa_code:
                self.serialized_processes[wf_id] = workflow.user_storage[1]
            else:
                QMessageBox.warning(self, "Workflow loading impossible",
                                    "The workflow was not created from a BrainVISA pipeline.")
                return

        serialized_process = self.serialized_processes[wf_id]
        serialized_process = StringIO(serialized_process)
        try:
            QtGui.QApplication.setOverrideCursor(
                QtGui.QCursor(QtCore.Qt.WaitCursor))
            view = SomaWorkflowProcessView(self.model,
                                           wf_id,
                                           self.model.current_resource_id,
                                           serialized_process=serialized_process,
                                           parent=_mainWindow)
            view.setAttribute(QtCore.Qt.WA_DeleteOnClose)
            QtGui.QApplication.restoreOverrideCursor()
        except Exception as e:
            QtGui.QApplication.restoreOverrideCursor()
            raise e

        view.show()


class WorkflowSubmissionDlg(QDialog):

    def __init__(self, parent=None):
        super(WorkflowSubmissionDlg, self).__init__(parent)

        from brainvisa.workflow import ProcessToSomaWorkflow

        loadUi(
            os.path.join(os.path.dirname(__file__), 'sw_submission_dlg.ui'),
                   self)
        # self.setupUi(self)

        resource_list = _computing_resource_pool.resource_ids()
        self.combo_resource.addItems(resource_list)
        current_resource_index = resource_list.index(
            _workflow_application_model.current_resource_id)
        self.combo_resource.setCurrentIndex(current_resource_index)
        self.resource_changed(current_resource_index)

        kind_of_file_processing = [ProcessToSomaWorkflow.NO_FILE_PROCESSING,
                                   ProcessToSomaWorkflow.FILE_TRANSFER,
                                   ProcessToSomaWorkflow.SHARED_RESOURCE_PATH]
        self.combo_out_files.addItems(kind_of_file_processing)
        kind_of_file_processing.append(ProcessToSomaWorkflow.BV_DB_SHARED_PATH)
        self.combo_in_files.addItems(kind_of_file_processing)

        self.lineedit_wf_name.setText("")
        self.dateTimeEdit_expiration.setDateTime(
            datetime.now() + timedelta(days=5))

        self.combo_resource.currentIndexChanged.connect(self.resource_changed)

    @QtCore.Slot(int)
    def resource_changed(self, resource_index):

        resource_id = self.combo_resource.currentText()
        queues = ["default queue"]
        queues.extend(
            _computing_resource_pool.connection(resource_id).config.get_queues())
        self.combo_queue.clear()
        self.combo_queue.addItems(queues)


class SomaWorkflowProcessView(QMainWindow):

    workflow_id = None

    resource_id = None

    model = None

    process = None

    serialized_process = None

    ui = None

    process_view = None

    workflow_tree_view = None

    workflow_item_view = None

    workflow_plot_view = None

    action_monitor_workflow = None

    workflow_menu = None

    workflow_tool_bar = None

    def __init__(self,
                 model,
                 workflow_id,
                 resource_id,
                 process=None,
                 serialized_process=None,
                 parent=None):
        super(SomaWorkflowProcessView, self).__init__(parent)

        Ui_SWProcessView = loadUiType(os.path.join(os.path.dirname(__file__),
                                                   'sw_process_view.ui'))[0]

        self.ui = Ui_SWProcessView()
        self.ui.setupUi(self)

        self.model = model
        self.serialized_process = serialized_process
        self.process = process
        self.workflow_id = workflow_id
        self.resource_id = resource_id

        self.model.current_workflow_changed.connect(
            self.current_workflow_changed)

        self.setCorner(QtCore.Qt.TopLeftCorner, QtCore.Qt.LeftDockWidgetArea)
        self.setCorner(QtCore.Qt.TopRightCorner, QtCore.Qt.RightDockWidgetArea)
        self.setCorner(
            QtCore.Qt.BottomLeftCorner, QtCore.Qt.LeftDockWidgetArea)
        self.setCorner(
            QtCore.Qt.BottomRightCorner, QtCore.Qt.RightDockWidgetArea)

        self.action_monitor_workflow = QAction(_t_('Monitor execution'), self)
        self.action_monitor_workflow.setCheckable(True)
        self.action_monitor_workflow.setIcon(
            QIcon(os.path.join(os.path.dirname(soma_workflow.gui.__file__), "icon/monitor_wf.png")))
        self.action_monitor_workflow.toggled.connect(
            self.enable_workflow_monitoring)
        self.action_monitor_workflow.setChecked(True)

        self.workflow_tree_view = soma_workflow.gui.workflowGui.WorkflowTree(
            _workflow_application_model,
                          assigned_wf_id=self.workflow_id,
                          assigned_resource_id=self.resource_id,
                          parent=self)

        self.workflow_item_view = soma_workflow.gui.workflowGui.WorkflowElementInfo(
            model=_workflow_application_model,
                        proxy_model=self.workflow_tree_view.proxy_model,
                        parent=self)

        self.workflow_plot_view = soma_workflow.gui.workflowGui.WorkflowPlot(
            _workflow_application_model,
                        assigned_wf_id=self.workflow_id,
                        assigned_resource_id=self.resource_id,
                        parent=self)
        if not soma_workflow.gui.workflowGui.MATPLOTLIB:
            self.ui.dock_plot.hide()
            self.ui.dock_plot.toggleViewAction().setVisible(False)

        self.workflow_info_view = soma_workflow.gui.workflowGui.WorkflowInfoWidget(
            _workflow_application_model,
                        assigned_wf_id=self.workflow_id,
                        assigned_resource_id=self.resource_id,
                        parent=self)

        self.workflow_menu = self.ui.menubar.addMenu("&Workflow")
        self.workflow_menu.addAction(_mainWindow.sw_widget.ui.action_stop_wf)
        self.workflow_menu.addAction(_mainWindow.sw_widget.ui.action_restart)
        _addSeparator(self.workflow_menu)
        self.workflow_menu.addAction(
            _mainWindow.sw_widget.ui.action_transfer_infiles)
        self.workflow_menu.addAction(
            _mainWindow.sw_widget.ui.action_transfer_outfiles)
        _addSeparator(self.workflow_menu)
        self.workflow_menu.addAction(
            _mainWindow.sw_widget.ui.action_delete_workflow)
        self.workflow_menu.addAction(
            _mainWindow.sw_widget.ui.action_change_expiration_date)

        view_menu = self.ui.menubar.addMenu("&View")
        view_menu.addAction(self.ui.dock_bv_process.toggleViewAction())
        view_menu.addAction(self.ui.dock_plot.toggleViewAction())
        view_menu.addAction(close_viewers_action(self))

        self.action_update_databases = QAction(self)
        self.action_update_databases.setText(_t_('Check && update databases'))
        self.action_update_databases.triggered.connect(self.update_databases)
        self.process_menu = self.ui.menubar.addMenu("&Process")
        self.process_menu.addAction(self.action_update_databases)

        self.workflow_tool_bar = QToolBar(self)
        self.workflow_tool_bar.addWidget(
            self.workflow_info_view.ui.wf_status_icon)
        _addSeparator(self.workflow_tool_bar)
        self.workflow_tool_bar.addAction(
            _mainWindow.sw_widget.ui.action_stop_wf)
        self.workflow_tool_bar.addAction(
            _mainWindow.sw_widget.ui.action_restart)
        _addSeparator(self.workflow_tool_bar)
        self.workflow_tool_bar.addAction(
            _mainWindow.sw_widget.ui.action_transfer_infiles)
        self.workflow_tool_bar.addAction(
            _mainWindow.sw_widget.ui.action_transfer_outfiles)

        self.ui.tool_bar.addWidget(self.workflow_tool_bar)
        _addSeparator(self.ui.tool_bar)
        self.ui.tool_bar.addAction(self.ui.dock_bv_process.toggleViewAction())
        _addSeparator(self.ui.tool_bar)
        self.ui.tool_bar.addAction(self.action_monitor_workflow)

        tree_widget_layout = QtGui.QVBoxLayout()
        tree_widget_layout.setContentsMargins(2, 2, 2, 2)
        tree_widget_layout.addWidget(self.workflow_tree_view)
        self.ui.centralwidget.setLayout(tree_widget_layout)

        self.process_layout = QtGui.QVBoxLayout()
        self.process_layout.setContentsMargins(2, 2, 2, 2)
        self.ui.dock_bv_process_contents.setLayout(self.process_layout)

        item_info_layout = QtGui.QVBoxLayout()
        item_info_layout.setContentsMargins(2, 2, 2, 2)
        item_info_layout.addWidget(self.workflow_item_view)
        self.ui.dock_item_info_contents.setLayout(item_info_layout)

        wf_info_layout = QtGui.QVBoxLayout()
        wf_info_layout.setContentsMargins(2, 2, 2, 2)
        wf_info_layout.addWidget(self.workflow_info_view)
        self.ui.dock_workflow_info_contents.setLayout(wf_info_layout)

        plot_layout = QtGui.QVBoxLayout()
        plot_layout.setContentsMargins(2, 2, 2, 2)
        plot_layout.addWidget(self.workflow_plot_view)
        self.ui.dock_plot_contents.setLayout(plot_layout)

        self.workflow_tree_view.selection_model_changed.connect(
            self.workflow_item_view.setSelectionModel)

        self.workflow_item_view.connection_closed_error.connect(
            _mainWindow.sw_widget.reconnectAfterConnectionClosed)

        self.workflow_tree_view.current_workflow_changed()
        self.workflow_plot_view.current_workflow_changed()
        self.workflow_info_view.current_workflow_changed()

        self.ui.dock_bv_process.toggleViewAction().toggled.connect(
            self.show_process)
        self.ui.dock_bv_process.toggleViewAction().setIcon(
            QIcon(os.path.join(neuroConfig.iconPath, 'icon.png')))

        self.ui.dock_plot.close()
        self.ui.dock_bv_process.close()
        self.ui.dock_workflow_info.close()

        wf_name = self.workflow_info_view.ui.wf_name.text()
        if len(wf_name[len(SomaWorkflowWidget.brainvisa_code):]) == 0:
            title = repr(self.workflow_id) + "@" + self.resource_id
        else:
            title = wf_name[
                len(SomaWorkflowWidget.brainvisa_code):] + "@" + self.resource_id
        self.setWindowTitle(title)

        # warning message in the status bar about the need to check and update
        # databases after execution
        warningMsg = QLabel(
            "<div style='font-size:9pt'><i><font color='red'>Warning:</font> After execution with Soma-Workflow, the databases may need to be updated.<br>Use \"Process -> Check &amp; update databases\" menu to do it.</i></div>")
        warningMsg.setWordWrap(True)
        self.ui.statusbar.addPermanentWidget(warningMsg, 1)

    def closeEvent(self, event):
        if self.process_view:
            self.process_view.cleanup()
        QMainWindow.closeEvent(self, event)

    @QtCore.Slot(bool)
    def enable_workflow_monitoring(self, enable):
        if not enable:
            if self.model.current_wf_id == self.workflow_id and \
               self.model.current_resource_id == self.resource_id:
                self.model.clear_current_workflow()
        else:
            if self.resource_id != self.model.current_resource_id:
                if self.model.resource_pool.resource_exist(self.resource_id):
                    self.model.set_current_connection(self.resource_id)
                else:
                    (resource_id,
                     new_connection) = _mainWindow.sw_widget.createConnection(self.resource_id,
                                                                              editable_resource=False)
                    if new_connection:
                        self.model.add_connection(resource_id, new_connection)
                    else:
                        QMessageBox.warning(
                            self, "Monitoring impossible", "The connection is not active.")
                        self.action_monitor_workflow.setChecked(False)
                        return

            if self.model.is_loaded_workflow(self.workflow_id):
                self.model.set_current_workflow(self.workflow_id)
            else:
                QMessageBox.warning(
                    self, "Monitoring impossible", "The workflow was deleted.")
                self.action_monitor_workflow.setChecked(False)

    @QtCore.Slot(bool)
    def show_process(self, checked):
        if self.process == None and self.serialized_process == None:
            return
        if checked and self.process_view == None:
            if self.process == None:
                # print("before unserialize")
                QtGui.QApplication.setOverrideCursor(
                    QtGui.QCursor(QtCore.Qt.WaitCursor))
                # self.ui.statusbar.showMessage("Unserialize...")
                try:
                    self.process = brainvisa.processes.getProcessInstance(
                        self.serialized_process)
                finally:
                    # self.ui.statusbar.clearMessage()
                    QtGui.QApplication.restoreOverrideCursor()
                # print("after unserialize")
            # print("before process view creation")
            QtGui.QApplication.setOverrideCursor(
                QtGui.QCursor(QtCore.Qt.WaitCursor))
            # self.ui.statusbar.showMessage("Building the process view...")
            try:
                self.process_view = ProcessView(self.process, parent=self,
                                                read_only=True)
            finally:
                QtGui.QApplication.restoreOverrideCursor()
            self.process_view.inlineGUI.hide()
            self.process_view.info.hide()
            self.process_view.eTreeWidget.setOrientation(Qt.Vertical)
            self.process_layout.addWidget(self.process_view)
            # print("After process view creation")
            self.ui.dock_bv_process.toggleViewAction().toggled.disconnect(
                self.show_process)

            process_button_layout = QtGui.QHBoxLayout()
            process_button_layout.setContentsMargins(2, 2, 2, 2)
            self.process_layout.addLayout(process_button_layout)

            self.process_view.action_clone_process.setText("Edit...")
            self.process_view.action_iterate.setText("Iterate...")

            btn_clone = QToolButton(self)
            btn_clone.setDefaultAction(self.process_view.action_clone_process)
            btn_clone.setMinimumWidth(90)
            btn_clone.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            process_button_layout.addWidget(btn_clone)

            btn_iterate = QToolButton(self)
            btn_iterate.setDefaultAction(self.process_view.action_iterate)
            btn_iterate.setMinimumWidth(90)
            btn_iterate.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            process_button_layout.addWidget(btn_iterate)

            btn_save = QToolButton(self)
            btn_save.setDefaultAction(self.process_view.action_save_process)
            btn_save.setMinimumWidth(90)
            btn_save.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            process_button_layout.addWidget(btn_save)

            self.process_menu.addAction(self.process_view.action_save_process)
            self.process_menu.addAction(self.process_view.action_clone_process)
            self.process_menu.addAction(self.process_view.action_iterate)

    @QtCore.Slot()
    def update_databases(self):
        QtGui.QApplication.setOverrideCursor(
            QtGui.QCursor(QtCore.Qt.WaitCursor))
        for dbSettings in neuroConfig.dataPath:
            try:
                if not dbSettings.builtin:
                    db = neuroHierarchy.databases.database(
                        dbSettings.directory)
                    brainvisa.processes.defaultContext().write(
                        "Updating database " + db.name + "...")
                    # first update before checking for missing referentials
                    db.clear(context=brainvisa.processes.defaultContext())
                    db.update(context=brainvisa.processes.defaultContext())
                    # check referentials and transformations information, the
                    # database will be updated after
                    brainvisa.processes.defaultContext().write(
                        "Checking referentials information in database " + db.name + "...")
                    checker = BVChecker_3_1(
                        db, brainvisa.processes.defaultContext())
                    checker.findActions()
                    checker.process()
            except:
                neuroException.showException(
                    beforeError="Error while updating database " + dbSettings.directory)
        QtGui.QApplication.restoreOverrideCursor()

    @QtCore.Slot()
    def current_workflow_changed(self):
        if self.model.current_wf_id == self.workflow_id and \
                self.model.current_resource_id == self.resource_id:
            self.action_monitor_workflow.setChecked(True)
            self.workflow_tool_bar.setEnabled(True)
            self.workflow_menu.setEnabled(True)
        else:
            self.action_monitor_workflow.setChecked(False)
            self.workflow_tool_bar.setEnabled(False)
            self.workflow_menu.setEnabled(False)


_aboutWidget = None
#----------------------------------------------------------------------------


class AboutWidget(QWidget):

    def __init__(self, parent=None, name=None):
        QWidget.__init__(self, parent)
        if name:
            self.setObjectName(name)
        layout = QVBoxLayout()
        self.setLayout(layout)
        self.setBackgroundRole(QPalette.Base)
        self.setAutoFillBackground(True)
        hb = QHBoxLayout()
        layout.addLayout(hb)
        layout.setContentsMargins(10, 10, 10, 10)
        self.setWindowTitle(_t_('About'))
        if getattr(AboutWidget, 'pixIcon', None) is None:
            setattr(AboutWidget, 'pixIcon', QIcon(
                os.path.join(neuroConfig.iconPath, 'icon.png')))
        self.setWindowIcon(self.pixIcon)

        def buildImageWidget(parent, fileName, desiredHeight=0):
            widget = QLabel(parent)
            # widget.setBackgroundRole( QPalette.Base )
            widget.setAlignment(Qt.AlignCenter)
            pixmap = QPixmap(os.path.join(neuroConfig.iconPath, fileName))
            if desiredHeight:
                stretch = float(desiredHeight) / pixmap.height()
                matrix = QTransform()
                matrix.scale(stretch, stretch)
                pixmap = pixmap.transformed(matrix)
            widget.setPixmap(pixmap)
            return widget

        widget = buildImageWidget(None, 'brainvisa.png')
        hb.addWidget(widget)
        label = QLabel(neuroConfig.versionText())
        hb.addWidget(label)
        # label.setBackgroundRole( QPalette.Base )
        label.setContentsMargins(4, 4, 4, 4)
        font = QFont()
        font.setPointSize(30)
        label.setFont(font)

        vb = QVBoxLayout()
        hb.addLayout(vb)
        widget = buildImageWidget(None, 'ifr49.png', desiredHeight=60)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'neurospin.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'shfj.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'mircen.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'inserm.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'cnrs.png', desiredHeight=60)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'chups.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)
        widget = buildImageWidget(None, 'parietal.png', desiredHeight=40)
        vb.addWidget(widget)
        widget.setContentsMargins(5, 5, 5, 5)

        if parent is None:
            px = (neuroConfig.qtApplication.desktop()
                  .width() - self.sizeHint().width()) / 2
            py = (neuroConfig.qtApplication.desktop()
                  .height() - self.sizeHint().height()) / 2
            self.setGeometry(
                px, py, self.sizeHint().width(), self.sizeHint().height())
            self.btnClose = QPushButton(_t_('Close'))
            layout.addWidget(self.btnClose)
            self.btnClose.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            self.btnClose.setFocus()
            self.btnClose.clicked.connect(self.close)


def aboutRequest():
    global _aboutWidget
    if _aboutWidget is None:
        _aboutWidget = AboutWidget()
    _aboutWidget.show()
    _aboutWidget.raise_()

#----------------------------------------------------------------------------


def logRequest():
    neuroLogGUI.LogViewer(neuroConfig.logFileName).show()

#----------------------------------------------------------------------------


def _addAction(parent, text=None, callback=None, shortcut=None):
# this 'strange' function is only here to avoid a memory leak in PyQt:
# it apparently does the same as QMenu.addAction() / addSeparator(),
# but if the former are called, the returned QAction is created in the
# C++ layer, and has a strange ownership side effect: the python part
# of the QAction is never destroyed.
# Creating the QAction from Python side seems to work around the problem.
# Seen in PyQt 4.9.3
    if text is None:
        ac = QAction(parent)
        ac.setSeparator(True)
    else:
        ac = QAction(text, parent)
    if callback is not None:
        ac.triggered.connect(callback)
    if shortcut is not None:
        ac.setShortcut(shortcut)
    parent.addAction(ac)
    return ac


def _addSeparator(menu):
    return _addAction(menu)

#----------------------------------------------------------------------------


def addBrainVISAMenu(widget, menuBar):
    bvMenu = QMenu("&BrainVISA", menuBar)  # avoid creating the menu in addMenu
    menuBar.addMenu(bvMenu)  # same problem as addAction()
    _addAction(bvMenu, _t_("&Help"), helpRequest, Qt.CTRL + Qt.Key_H)
    _addAction(bvMenu, _t_("About"), aboutRequest)
    _addAction(bvMenu)
    _addAction(bvMenu, _t_("&Preferences"),
               neuroConfigGUI.editConfiguration, Qt.CTRL + Qt.Key_P)
    _addAction(bvMenu, _t_("Show &Log"), logRequest, Qt.CTRL + Qt.Key_L)
    _addAction(bvMenu, _t_("&Open process..."), ProcessView.open,
               Qt.CTRL + Qt.Key_O)
    _addAction(bvMenu, _t_("Load process setups"), loadProcessSetupsGUI)
    _addAction(bvMenu, _t_("Reload toolboxes"), reloadToolboxesGUI)
    _addAction(bvMenu, _t_("Start &Shell"), startShell, Qt.CTRL + Qt.Key_S)
    _addAction(bvMenu)
    if not isinstance(widget, ProcessSelectionWidget):
        _addAction(bvMenu, _t_("Close"), widget.close, Qt.CTRL + Qt.Key_W)
    _addAction(bvMenu, _t_("&Quit"), quitRequest, Qt.CTRL + Qt.Key_Q)
    return bvMenu


#----------------------------------------------------------------------------
class HTMLBrowser(QWidget):

    class BVTextBrowser(WebBrowserWithSearch):

        def __init__(self, parent, name=None):
            WebBrowserWithSearch.__init__(self, parent)
            if name:
                self.setObjectName(name)
            # self.mimeSourceFactory().setExtensionType("py", "text/plain")
            self.openWebAction = QAction(_t_('Open in a web browser'), self)
            self.openWebAction.setShortcut(Qt.CTRL + Qt.Key_W)
            self.openWebAction.triggered.connect(self.openWeb)
            self.openLinkWebAction = QAction(
                _t_('Open link in a web browser'), self)
            self.openLinkWebAction.triggered.connect(self.openLinkWeb)
            self.openLinkInTextEditorAction = QAction(
                _t_('Open link in a text editor'), self)
            self.openLinkInTextEditorAction.triggered.connect(
                self.openSourceLink)
            self.copyLinkAction = QAction(_t_('Copy link URL'), self)
            self.copyLinkAction.triggered.connect(self.copyLinkUrl)
            self.linkUrl = None

        def setSource(self, url):
            text = url.toString()
            bvp = unicode(text)
            if bvp.startswith('bvshowprocess://'):
                bvp = bvp[16:]
                # remove tailing '/'
                if bvp[-1] == '/':
                    bvp = bvp[: -1]
                proc = brainvisa.processes.getProcess(bvp)
                if proc is None:
                    print('No process of name', bvp)
                else:
                    win = ProcessView(proc())
                    win.show()
            elif bvp.startswith('file://') and bvp.endswith('.py'):
                WebBrowserWithSearch.setSource(self, url)
                self.setHtml('<html><body><pre>' + htmlEscape(
                    open(url.toLocalFile()).read()) + '</pre></body></html>')
                sys.stdout.flush()
            else:
                WebBrowserWithSearch.setSource(self, url)
            if not use_webengine:
                self.page().setLinkDelegationPolicy(
                    QWebEnginePage.DelegateAllLinks)

        def contextMenuEvent(self, event):
            rel_pos = event.pos()
            if use_webengine:
                cmdata = self.page().contextMenuData()
                if not cmdata.isValid():
                    return
                hit_url = cmdata.linkUrl()
                if hit_url is not None \
                        and (not hit_url.isValid() or hit_url.isEmpty()):
                    hit_url = None
            else:
                main_frame = self.page().mainFrame()
                hit_test = main_frame.hitTestContent(rel_pos)
                hit_url = hit_test.linkUrl()
                if hit_url.isEmpty():
                    hit_url = None
            menu = self.customMenu(hit_url)
            menu.exec_(event.globalPos())

        def customMenu(self, hit_url=None):
            menu = WebBrowserWithSearch.customMenu(self)
            menu.addSeparator()
            menu.addAction(self.openWebAction)
            self.linkUrl = hit_url
            if hit_url:
                menu.addSeparator()
                menu.addAction(self.openLinkWebAction)
                if unicode(hit_url.toString()).endswith('.py'):
                    menu.addAction(self.openLinkInTextEditorAction)
                menu.addAction(self.copyLinkAction)
            return menu

        def copyLinkUrl(self, dummy):
            QApplication.clipboard().setText(self.linkUrl.toString())

        def openLinkWeb(self, dummy):
            openWeb(self.linkUrl.toString())

        def openSourceLink(self, dummy):
            configuration = Application().configuration
            textEditor = configuration.brainvisa.textEditor
            if textEditor is not None:
                textEditor = distutils.spawn.find_executable(textEditor)
                if textEditor:
                    env = os.environ.copy()
                    if not textEditor.startswith(os.path.dirname(neuroConfig.mainPath)):
                        # external command
                        if neuroConfig.brainvisaSysEnv:
                            env.update(
                                neuroConfig.brainvisaSysEnv.getVariables())
                    os.spawnle(
                        os.P_NOWAIT, textEditor, textEditor,
                      unicode(self.linkUrl.toString()), env)

        def openWeb(self):
            openWeb(self.url().toString())

    def __init__(self, parent=None, name=None, fl=Qt.WindowFlags()):
        QWidget.__init__(self, parent, fl)
        if name:
            self.setObjectName(name)

        vbox = QVBoxLayout(self)
        self.setLayout(vbox)
        vbox.setSpacing(2)
        vbox.setContentsMargins(3, 3, 3, 3)

        if getattr(HTMLBrowser, 'pixHome', None) is None:
            setattr(HTMLBrowser, 'pixIcon', QIcon(
                os.path.join(neuroConfig.iconPath, 'icon_help.png')))
            setattr(HTMLBrowser, 'pixHome', QIcon(
                os.path.join(neuroConfig.iconPath, 'top.png')))

        self.setWindowIcon(HTMLBrowser.pixIcon)

        hbox = QHBoxLayout()
        hbox.setSpacing(6)
        hbox.setContentsMargins(0, 0, 0, 0)

        self.homeAction = QAction(_t_('Home'), self)
        self.homeAction.setIcon(self.pixHome)
        btnHome = QToolButton()
        btnHome.setSizePolicy(
            QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum))
        hbox.addWidget(btnHome)

        btnBackward = QToolButton()
        btnBackward.setSizePolicy(
            QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum))
        hbox.addWidget(btnBackward)

        btnForward = QToolButton()
        btnForward.setSizePolicy(
            QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum))
        hbox.addWidget(btnForward)

        btnReload = QToolButton()
        btnReload.setSizePolicy(
            QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum))
        hbox.addWidget(btnReload)

        vbox.addLayout(hbox)

        browser = self.BVTextBrowser(self)
        browser.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding))
        vbox.addWidget(browser)

        self.homeAction.triggered.connect(self.home)

        btnHome.setDefaultAction(self.homeAction)
        btnForward.setDefaultAction(browser.pageAction(QWebEnginePage.Forward))
        btnBackward.setDefaultAction(browser.pageAction(QWebEnginePage.Back))
        a = browser.pageAction(QWebEnginePage.Reload)
        a.setShortcut(QtGui.QKeySequence.Refresh)
        btnReload.setDefaultAction(a)
        browser.linkClicked.connect(browser.setSource)

        self.browser = browser

        neuroConfig.registerObject(self)

        hbox.addWidget(QLabel(_t_('Search site:')))
        self._siteSearch = QLineEdit()
        hbox.addWidget(self._siteSearch)
        self._siteSearch.returnPressed.connect(self.siteSearch)

    def setSource(self, source):
        if not isinstance(source, QUrl):
            if os.path.exists(source):
                source = QUrl.fromLocalFile(source)
            else:
                source = QUrl(source)
        self.browser.setSource(source)

    def reload(self):
        self.browser.reload()

    def setText(self, text):
        self.browser.setHtml(text)

    def openWeb(self):
        self.browser.openWeb()

    def showCategoryDocumentation(self, category):
        """
        Searches for a documentation file associated to this category and opens it  in this browser.
        Documentation files are in docPath/processes/categories/category. Category is a relative path, so if no documentation is found with the entire path, it removes the first item and retries.
        For example, if "t1 mri/viewers" doesn't exists, tries "viewers".
        If there is no documentation file, the browser page is empty.
        """
        categoryPath = category.lower().split("/")
        found = False
        while ((len(categoryPath) > 0) and not found):
            html = neuroConfig.getDocFile(
                os.path.join(os.path.join('processes', 'categories', *categoryPath), 'category_documentation.html'))
            if os.path.exists(html):
                self.setSource(html)
                found = True
            categoryPath = categoryPath[1:]

        if not found:
            self.browser.setHtml('')

    def closeEvent(self, event):
        neuroConfig.unregisterObject(self)
        QWidget.closeEvent(self, event)

    def home(self, void=None):
        newver = checkbrainvisaupdates.checkUpdates()
        if newver:
            text = '''<html><div style="background-color: #ffe8e8">
<hr/>
<h2>A newer BrainVISA version is available</h2>
<p>Version ''' + '.'.join( [ str(x) for x in newver[0] ] ) + ''' is available on the BrainVISA web site.<br/>
Download it on <a href="http://brainvisa.info/downloadpage.html">the BrainVISA download page</a>.</p>
<hr/>
</div>
<iframe src="file://''' + neuroConfig.getDocFile(os.path.join( 'help', 'index.html' ) ) + '''" align="center" width="100%" height="100%" scrolling="auto" frameborder="0"></iframe>
</html>'''
            tmp = brainvisa.processes.defaultContext().temporary('HTML')
            open(tmp.fullPath(), 'w').write(text)
            self.setSource(tmp.fullPath())
        else:
            self.setSource(neuroConfig.getDocFile(
                os.path.join('help', 'index.html')))

    def siteSearch(self):
        '''Search the brainvisa.info website using google search'''
        if self._siteSearch.text():
            if not hasattr(self, '_currentNonSearchPage'):
                self._currentNonSearchPage = self.browser.url()
            url = 'http://www.google.com/cse?url=brainvisa.info&cref=http%3A%2F%2Fwww.google.com%2Fcse%2Ftools%2Fmakecse%3Furl%3Dbrainvisa.info&ie=&q=' + \
                urllib.quote_plus(self._siteSearch.text())
            self.setSource(QUrl.fromEncoded(url))
        else:
            # get back to the previous non-search page
            url = self._currentNonSearchPage
            del self._currentNonSearchPage
            self.setSource(url)


#----------------------------------------------------------------------------
class NamedPushButton(QPushButton):

    def __init__(self, parent, name):
        QPushButton.__init__(self, parent, name)
        self.clicked.connect(self._pyClicked)

    def _pyClicked(self):
        self.clicked.emit(unicode(self.name()))


#----------------------------------------------------------------------------
class WidgetScrollV(QScrollArea):
    # class VBox( QWidget ):
        # def __init__( self, parent, ws ):
            # QWidget.__init__( self, parent )
            # layout=QVBoxLayout()
            # self.setLayout(layout)
            # layout.setSpacing( 5 )
            # self.ws = ws

        # def sizeHint( self ):
            # return QSize( self.ws.visibleWidth(), QWidget.sizeHint( self
            # ).height() )

    def __init__(self, parent=None, name=None):
        # self.box = None
        QScrollArea.__init__(self, parent)
        if name:
            self.setObjectName(name)
        # self.setHorizontalScrollBarPolicy( Qt.ScrollBarAlwaysOff )
        self.setFrameStyle(self.NoFrame)
        # self.box = self.VBox( None, self )
        # self.setWidget( self.box )
        self.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding))
        self.setWidgetResizable(True)
        # self.box.resize( self.visibleWidth(), self.box.height() )

    # def show( self ):
        # self.setMaximumSize( self.maximumWidth(), self.box.size().height() )
        # return QScrollArea.show( self )

    # def resizeEvent( self, e ):
        # result = QScrollArea.resizeEvent( self, e )
        # if self.widget():
            # self.widget().resize( self.visibleWidth(), self.box.height() )
            # self.updateGeometry()
            # self.setMaximumSize( self.maximumWidth(), self.box.sizeHint().height() )
            # if self.box.width() != self.visibleWidth():
                # self.box.resize( self.visibleWidth(), self.box.height() )
                # self.updateGeometry()
        # return result

    # def sizeHint( self ):
        # if self.box:
            # return QWidget.sizeHint( self.box )
        # else:
            # return QScrollArea.sizeHint( self )

    # def visibleWidth(self):
        # return self.viewport().width()

#----------------------------------------------------------------------------


class ExecutionContextGUI(brainvisa.processes.ExecutionContext):

    def __init__(self):
        brainvisa.processes.ExecutionContext.__init__(self)

    def ask(self, message, *buttons, **kwargs):
        modal = kwargs.get("modal", 1)
        dlg = self.dialog(modal, message, None, *buttons)
        return mainThreadActions().call(dlg.call)

    def dialog(self, parentOrFirstArgument, *args, **kwargs):
        if isinstance( parentOrFirstArgument, QWidget ) or \
           parentOrFirstArgument is None:
            return self._dialog(parentOrFirstArgument, *args, **kwargs)
        else:
            return self._dialog(*((None, parentOrFirstArgument) + args), **kwargs)

    def _dialog(self, parent, modal, message, signature, *buttons):
        return mainThreadActions().call(UserDialog, parent, modal,
                                        message, signature, buttons)

    def mainThreadActions(self):
        '''Returns an object which allows to pass actions to be executed in the main thread. Its implementation may differ according to the presence of a running graphics event loop, thus the returned object may be an instance of different classes: :py:class:`soma.qtgui.api.QtThreadCall`, :py:class:`soma.qtgui.api.FakeQtThreadCall`, or even something else.

        In any case the returned *mainthreadactions* object has 2 methods, *call()* and *push()*:

        ::

          result = mainthreadactions.call(function, *args, **kwargs)
          #or
          mainthreadactions.push(function, *args, **kwargs)
        '''
        return mainThreadActions()

    def showProgress(self, value, maxval=None):
        def setProgress(self, value, maxval):
            if not maxval:
                maxval = 100
            if not hasattr(self, '_progressBar'):
                if not hasattr(self, 'inlineGUI'):
                    # no GUI: fallback to text mode
                    brainvisa.processes.ExecutionContext.showProgress(
                        self, value, maxval)
                    return
                layout = self.inlineGUI.parentWidget().layout()
                self._progressBar = QProgressBar(None)
                layout.addWidget(self._progressBar)
                self._progressBar.show()
            if self._progressBar.maximum() != maxval:
                self._progressBar.setRange(0, maxval)
            self._progressBar.setValue(int(round(value)))
        mainThreadActions().push(setProgress, self, value, maxval)

    @staticmethod
    def createContext():
        return ExecutionContextGUI()

#----------------------------------------------------------------------------


class ExecutionNodeGUI(QWidget):

    def __init__(self, parent, parameterized, read_only=False):
        QWidget.__init__(self, parent)
        layout = QVBoxLayout()
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(4)
        self.setLayout(layout)
        self.parameterizedWidget = ParameterizedWidget(parameterized, None)
        if read_only:
            self.parameterizedWidget.set_read_only(True)
        layout.addWidget(self.parameterizedWidget)

    def closeEvent(self, event):
        self.parameterizedWidget.close()
        QWidget.closeEvent(self, event)

    def _checkReadable(self):
        if self.parameterizedWidget is not None:
            self.parameterizedWidget.checkReadable()

#----------------------------------------------------------------------------


class VoidClass:
    pass


#----------------------------------------------------------------------------
class RadioItem(QWidget):

    """An custom item to replace a QTreeWidgetItem for the representation of a SelectionExecutionNode item with a radio button.
    QTreeWidgetItem enables only check box items."""

    def __init__(self, text, group, parent=None):
        QWidget.__init__(self, parent)
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.setLayout(layout)
        self.radio = QRadioButton()
        self.radio.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)
        layout.addWidget(self.radio)
        group.addButton(self.radio)
        self.icon = QLabel()
        layout.addWidget(self.icon)
        self.label = QLabel(text)
        layout.addWidget(self.label)
        layout.addStretch(1)
        self.setAutoFillBackground(True)
#    self.show()

    def setChecked(self, checked):
        self.radio.setChecked(checked)

    def isChecked(self):
        return self.radio.isChecked()

    def setIcon(self, icon):
        self.icon.setPixmap(icon.pixmap(*defaultIconSize))

#----------------------------------------------------------------------------


class NodeCheckListItem(QTreeWidgetItem):

    def __init__(self, node, parent, index=None, text=None, itemType=None,
                 read_only=False):
        if not index is None and index < parent.childCount():
            QTreeWidgetItem.__init__(self)
            parent.insertChild(index, self)
        else:
            QTreeWidgetItem.__init__(self, parent)

        self._node = node
        self.itemType = itemType
        # the following flag is needed to block itemChanged signal processing
        # before the item is completely setup, otherwise it will trigger a
        # node selection change instead of the contrary
        self.creating = True
        self.read_only = read_only
        if itemType == "radio" and not self.read_only:
            # if the item type is radio, create a custom item RadioItem to replace the current QTreeWidgetItem at display
            # the radio button is included in a button group that is registred
            # in the parent item
            buttonGroup = getattr(self.parent(), "buttonGroup", None)
            if not buttonGroup:
                buttonGroup = QButtonGroup()
                self.parent().buttonGroup = buttonGroup
            self.widget = RadioItem(text, buttonGroup)
            self.treeWidget().setItemWidget(self, 0, self.widget)
            self.widget.radio.clicked.connect(self.radioClicked)
            self.widget.radio.toggled.connect(self.radioToggled)
        else:  # not a radio button or read only, show text directly in the qtreeWidgetItem
            if text:
                self.setText(0, text)
        self.setOn(node._selected)
        node._selectionChange.add(self.nodeStateChanged)
        self.creating = False

    def radioClicked(self, checked):
        self.treeWidget().setCurrentItem(self)

    def radioToggled(self, checked):
        self.stateChange(checked)

    def itemClicked(self):
        if self.itemType == "check":
            self.stateChange(self.isOn())

    def setIcon(self, col, icon):
        if self.itemType == "radio":
            self.widget.setIcon(icon)
        else:
            QTreeWidgetItem.setIcon(self, col, icon)

    def stateChange(self, selected):
        self._node.setSelected(selected)

    def nodeStateChanged(self, node):
        self.setOn(node._selected)

    def cleanup(self):
        self._node._selectionChange.remove(self.nodeStateChanged)
        self._node = None

    def setOn(self, b):
        if self.read_only:
            if b:
                self.setFlags(
                    Qt.ItemIsSelectable | Qt.ItemIsDragEnabled | Qt.ItemIsEnabled)
            else:
                self.setFlags(Qt.ItemIsSelectable | Qt.ItemIsDragEnabled)
        elif self.itemType == "radio":
            self.widget.setChecked(b)
        elif self.itemType == "check":
            if b:
                self.setCheckState(0, Qt.Checked)
            else:
                self.setCheckState(0, Qt.Unchecked)

    def isOn(self):
        if self.read_only:
            return int(Qt.ItemIsEnabled & self.flags()) > 0
        elif self.itemType == "radio":
            return self.widget.isChecked()
        elif self.itemType == "check":
            return self.checkState(0) == Qt.Checked
        return True

    def check(self, b):
        """
        This method is used to check or uncheck a checkable item and warn the underlying model of the state change.
        It is useful for the feature select/unselect before/after/all in pipelines and iterations.
        """
        if self.itemType == "check" and not self.read_only:
            if b:
                self.setCheckState(0, Qt.Checked)
            else:
                self.setCheckState(0, Qt.Unchecked)
            self.stateChange(b)

    def currentItemChanged(self, current):
        """ This function is called when the item gains or lose the status of current item of the tree widget.
        In case the item is a radio button, its background and foreground colors are changed to follow the tree item widget policy.

        :param current: indicates if the item is the current item or not. boolean.
        """
        if self.itemType == "radio" and not self.read_only:
            if current:
                self.widget.setBackgroundRole(QPalette.Highlight)
                self.widget.setForegroundRole(QPalette.HighlightedText)
            else:
                self.widget.setBackgroundRole(QPalette.Base)
                self.widget.setForegroundRole(QPalette.Text)

#------------------------------------------------------------------------------


class ParameterLabel(QLabel):

    '''A QLabel that emits signal 'contextMenuEvent' whenever a
    contextMenuEvent occurs'''

    toggleDefault = QtCore.Signal(str)
    lock_system = QtCore.Signal(str)
    unlock_system = QtCore.Signal(str)

    def __init__(self, parameterName, mandatory, parent, userLevel=0):
        if mandatory:
            printParameterName = '<b>' + parameterName + ':</b>'
        else:
            printParameterName = parameterName + ':'

        from soma.wip.application.api import Application
        if Application().configuration.brainvisa.showParamUserLevel:
            if userLevel == 1:
                printParameterName = '<sup style="color:orange;">1</sup> ' + \
                    printParameterName
            elif userLevel == 2:
                printParameterName = '<sup style="color:red;">2</sup> ' + \
                    printParameterName
            elif userLevel >= 3:
                printParameterName = '<sup style="color:blue;">*</sup> ' + \
                    printParameterName

        QLabel.__init__(self, printParameterName, parent)

        # Save parameter name
        self.parameterName = parameterName
        self.printParameterName = printParameterName

        # Create popup menu
        self.contextMenu = QMenu(self)
        # self.contextMenu.setCheckable( True )
        self.default_id = _addAction(self.contextMenu, _t_('default value'),
                                     self.defaultChanged)
        self.default_id.setCheckable(True)
        self.default_id.setChecked(True)
        self.addMenuLock()
        self.setAutoFillBackground(True)
        self.setBackgroundRole(QPalette.Window)

    def set_read_only(self, read_only):
        self.default_id.setEnabled(not read_only)

    def contextMenuEvent(self, e):
        self.contextMenu.exec_(e.globalPos())
        e.accept()

    def paramLabelText(self,  val_default_id, val_lock_id):
        # CAS : val_default_id / val_lock_id
        # val_default_id = 0 pas valeur par defaut donc icone
        # val_default_id = 1 valeur par defaut donc pas icone
        text = ''  # cas 0 / 0
#    if
#        1 / 0
#        if 1 / 1
#    else 0 / 1

        if val_default_id:
            if val_lock_id:
                text =  text + '<img src="' + \
                    os.path.join(
                        neuroConfig.iconPath, 'lock.png') + '" height="16"/> '
        else:
            text =  '<img src="' + \
                os.path.join(
                    neuroConfig.iconPath, 'modified.png') + '" height="16"/> '
            if val_lock_id:
                text =  text + '<img src="' + \
                    os.path.join(
                        neuroConfig.iconPath, 'lock.png') + '" height="16"/> '

        return(text)

    def addMenuLock(self):
        self.lock_id = _addAction(self.contextMenu, _t_('lock'),
                                  self.lockChanged)
        self.lock_id.setCheckable(False)
        self.lock_id.setChecked(False)

    def readText(self, txt):
        """ Function to return the value of text without value of tag for images """
        if unicode(txt).startswith('<img src='):
            x = txt.find('/> ')
            txt = txt[x + 3:]
            return txt
        return txt

    def lockChanged(self, checked=False):
        """ This function is to lock or unlock data if a user click on the lock menu  """

        if checked:
            self.lock_system.emit(self.parameterName)
            # warning the value of lock_id can be become false if we can't write the lock file.
            # For example, if you try to lock a file which doesn't exist
        else:
            self.unlock_system.emit(self.parameterName)
            # txt = self.paramLabelText(self.default_id.isChecked(), False)

        # on remet a jour en fonction du resultat du unlock du fichier
        self.setlock(self.lock_id.isChecked())
        # label update is performed by setlock

    def setlock(self, default):
        """ This function is to set lock or unlock data """
        self.lock_id.setChecked(default)

        txt = self.paramLabelText(
            self.default_id.isChecked(), self.lock_id.isChecked())

        txt += self.printParameterName
        self.setText(unicode(txt))

    def defaultChanged(self, checked=False):
        # print("-- FUNCTION defaultChanged : neuroProcessesGUI /
        # ParameterLabel --", checked)
        self.toggleDefault.emit(self.parameterName)

        txt = self.paramLabelText(
            self.default_id.isChecked(), self.lock_id.isChecked())

        txt += self.printParameterName
        self.setText(unicode(txt))

    def setDefault(self, default):
        # print("-- FUNCTION setDefault : neuroProcessesGUI / ParameterLabel
        # --")
        self.default_id.setChecked(default)

        # self.lockChanged()

        txt = self.paramLabelText(
            self.default_id.isChecked(), self.lock_id.isChecked())

        txt += self.printParameterName
        self.setText(unicode(txt))

    # def defaultChanged( self, checked=False ):
        # self.default_id.toggle()
        # self.toggleDefault.emit(self.parameterName)
        # if self.default_id.isChecked():
            # txt = unicode( self.text() )
            # if txt.startswith( '<img src=' ):
            # x = txt.find( '/> ' )
            # txt = txt[ x+3 : ]
            # self.setText( txt )
        # else:
            # if not unicode( self.text() ).startswith( '<img src=' ):
            # self.setText( '<img src="' \
                    #+ os.path.join( neuroConfig.iconPath, 'modified.png' ) \
                    #+ '" height="16"/> ' + self.text() )

    # def setDefault( self, default ):
        # self.default_id.setChecked( default )
        # if default:
            # txt = unicode( self.text() )
            # if txt.startswith( '<img src=' ):
            # x = txt.find( '/> ' )
            # txt = txt[ x+3 : ]
            # self.setText( txt )
        # else:
            # if not unicode( self.text() ).startswith( '<img src=' ):
            # self.setText( '<img src="' \
                    #+ os.path.join( neuroConfig.iconPath, 'modified.png' ) \
                    #+ '" height="16"/> ' + self.text() )


#------------------------------------------------------------------------------
class ParameterizedWidget(QWidget):

    def __init__(self, parameterized, parent):
# lock#    if getattr( ParameterizedWidget, 'pixDefault', None ) is None:
# lock#      setattr( ParameterizedWidget, 'pixDefault', QPixmap( os.path.join( neuroConfig.iconPath, 'lock.png' ) ) )
# lock#      setattr( ParameterizedWidget, 'pixCustom', QPixmap(
# os.path.join( neuroConfig.iconPath, 'unlock.png' ) ) )

        QWidget.__init__(self, parent)
        self.destroyed.connect(self.cleanup)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        self.setLayout(layout)
        # the scroll widget will contain parameters widgets
        self.scrollWidget = WidgetScrollV()
        layout.addWidget(self.scrollWidget)

        # spacer = QSpacerItem( 0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding )
        # layout.addItem( spacer )

        # Using weakref here because self.parameterized may contain a reference
        # to self.parameterChanged (see below) which is a bound method that contains
        # a reference to self. If nothing is done self is never destroyed.

        self.parameterized = weakref.proxy(parameterized)
        self.parameterized.deleteCallbacks.append(self.parameterizedDeleted)
        self.labels = {}
        self.editors = {}
# lock#    self.btnLock={}
        self._currentDirectory = None

        self._doUpdateParameterValue = False
        first = None

        documentation = {}
        id = getattr(parameterized, '_id', None)
        if id is not None:
            procdoc = brainvisa.processes.readProcdoc(id)
            if procdoc:
                documentation = procdoc.get(neuroConfig.language)
                if documentation is None:
                    documentation = procdoc.get('en', {})

        # the widget that will contain parameters, it will be put in the scroll
        # widget
        parametersWidget = QWidget()
        # QGridLayout : a label and an editor for each parameter, in 2 columns
        parametersWidgetLayout = QFormLayout()
        parametersWidgetLayout.setContentsMargins(0, 0, 0, 0)
        if sys.platform == 'darwin' and QtCore.qVersion() == '4.6.2':
            # is this layout problem a bug in qt/Mac 4.6.2 ?
            parametersWidgetLayout.setSpacing(0)
        else:
            parametersWidgetLayout.setSpacing(2)
        parametersWidget.setLayout(parametersWidgetLayout)
        # create a widget for each parameter

        # sort signature item by section title
        sectionTitleList = []  # useful to keep signature order
        sectionTitleSortedDict = {}
        for key, parameter in items(self.parameterized.signature):
            if neuroConfig.userLevel >= parameter.userLevel and parameter.visible:
                sectionTitle = parameter.getSectionTitleIfDefined()
                sectionTitleList.append(sectionTitle)
                if sectionTitle in sectionTitleSortedDict.keys():
                    sectionTitleSortedDict[
                        sectionTitle].append((key, parameter))
                else:
                    sectionTitleSortedDict[sectionTitle] = [(key, parameter)]

        # sectionTitleList = list( set( sectionTitleList ) )#this command do
        # not keep the list order
        uniqueSectionTitleList = []
        for sectionTitle in sectionTitleList:
            if not sectionTitle in uniqueSectionTitleList:
                uniqueSectionTitleList.append(sectionTitle)
            else:
                pass

        visible_sections = getattr(
            self.parameterized, 'visible_sections', None)

        # the parameters not grouped will be added first
        if None in uniqueSectionTitleList:
            uniqueSectionTitleList.insert(
                0, uniqueSectionTitleList.pop(uniqueSectionTitleList.index(None)))

        for sectionTitle in uniqueSectionTitleList:
            currentSectionTitle = None
            if len(uniqueSectionTitleList) > 1 and sectionTitle is not None:
                currentSectionTitle = SectionTitle(
                    sectionTitle,
                    collapsed=(visible_sections is not None
                               and sectionTitle not in visible_sections))
                parametersWidgetLayout.addRow(currentSectionTitle)

            for k, p in sectionTitleSortedDict[sectionTitle]:
                l = ParameterLabel(
                    k, p.mandatory, None, userLevel=p.userLevel)
                l.setDefault(self.parameterized.isDefault(k))
                l.toggleDefault.connect(self._toggleDefault)

                if isinstance(p, ReadDiskItem):
                    l.lock_id.setCheckable(True)
                    l.setlock(self._setlock_system(k))
                              # ini la valeur de lock du parametre
                    # l.setlock_system.connect(self._setlock_system)
                    l.lock_system.connect(self._lock_system)
                    l.unlock_system.connect(self._unlock_system)

                l.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
                self.labels[k] = l
                e = p.editor(None, k, weakref.proxy(self))
                if isinstance(e, neuroDataGUI.DataEditor):
                    # Complete popup menu with specific actions of the editor
                    e.createPopupMenu(l.contextMenu)

                # if p.getSectionTitleIfDefined() is not None:
                # parametersWidgetLayout.addRow( SectionTitle(
                # p.getSectionTitleIfDefined() ) )

                parametersWidgetLayout.addRow(l, e)
                if currentSectionTitle is not None:
                    currentSectionTitle.addSectionWidgets(l, e)

                self.parameterized.addParameterObserver(
                    k, self.parameterChanged)

                self.editors[k] = e
                if first is None:
                    first = e
                v = getattr(self.parameterized, k, None)
                if v is not None:
                    self.setValue(k, v, 1)
                e.valuePropertiesChanged(self.parameterized.isDefault(k))
                e.noDefault.connect(self.removeDefault)
                e.newValidValue.connect(self.updateParameterValue)
# lock#        btn = NamedPushButton( hb, k )
# lock#        btn.setPixmap( self.pixCustom )
# lock#        btn.setFocusPolicy( QWidget.NoFocus )
# lock#        btn.setToggleButton( 1 )
# lock#        btn.hide()
# lock#        btn.clicked.connect(self._toggleDefault)
# lock#        self.btnLock[ k ] = btn
                if documentation is not None:
                    self.setParameterToolTip(k,
                                             XHTML.html(
                                             documentation.get('parameters', {}).get(k, ''))
                                             + '<br/><img src="'
                                             + os.path.join(
                                                 neuroConfig.iconPath, 'modified.png')
                                             + '"/><em>: '
                                             + _t_(
                                             'value has been manually changed and is not modified by '
                                             'links anymore')
                                             + '</em>')

        parametersWidget.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum))
        self.scrollWidget.setWidget(parametersWidget)
        if first:
            first.setFocus()
        self._doUpdateParameterValue = True
        # self.scrollWidget.widget().resize(600, 200)
#    self.scrollWidget.show()

    def set_read_only(self, read_only):
        for x in self.editors.keys():
            self.editors[x].set_read_only(read_only)
        for x in self.labels.keys():
            self.labels[x].set_read_only(read_only)

    def parameterizedDeleted(self, parameterized):
        for k, p in items(parameterized.signature):
            try:
                parameterized.removeParameterObserver(
                    k, self.parameterChanged)
            except ValueError:
                pass
        self.parameterized = None
        for x in self.editors.keys():
            self.editors[x].releaseCallbacks()

    def __del__(self):
        if self.parameterized is not None:
            self.parameterizedDeleted(self.parameterized)
        # QWidget.__del__( self )

    def cleanup(self):
        for x in self.editors.keys():
            self.editors[x].releaseCallbacks()

    def closeEvent(self, event):
        for k, p in items(self.parameterized.signature):
            try:
                self.parameterized.removeParameterObserver(
                    k, self.parameterChanged)
            except ValueError:
                pass
        self.cleanup()
        QWidget.closeEvent(self, event)

    def setParameterToolTip(self, parameterName, text):
        self.labels[parameterName].setToolTip(html_with_local_images(
                                              self.parameterized.signature[parameterName].toolTipText(
                                              parameterName, text)))

    def parameterChanged(self, parameterized, parameterName, value):
        """This method is called when an attribute has changed in the model.
        A parameter can change in the model because it is links to another parameter that has changed or because the user changed it in the GUI."""
        # It is necessary to read user values before applying changes,
        # otherwise selected data are reset
        self.readUserValues()
        self._doUpdateParameterValue = False
        default = parameterized.isDefault(parameterName)
        self.setValue(parameterName, value, default=default)
        self.labels[parameterName].setDefault(default)
        self.editors[parameterName].valuePropertiesChanged(default)

        # lock system
        self.labels[parameterName].setlock(self._setlock_system(parameterName))

        self._doUpdateParameterValue = True

    def updateParameterValue(self, name, value):
        try:
            # lock system
            self.labels[name].setlock(self._setlock_system(name))

            if self._doUpdateParameterValue:
                setattr(self.parameterized, name, value)
        except:
            import traceback
            traceback.print_exc()

    def removeDefault(self, name):
        # lock system
        self.labels[name].setlock(self._setlock_system(name))

        self.parameterized.setDefault(name, False)
        self.labels[name].setDefault(False)
        self.editors[name].valuePropertiesChanged(False)
# lock#    self.btnLock[ name ].setPixmap( self.pixDefault )
# lock#    self.btnLock[ name ].setOn( 1 )
# lock#    self.btnLock[ name ].show()

    def _toggleDefault(self, name):
        isdefault = not self.parameterized.isDefault(name)
        value = getattr(self.parameterized, name)
        # trick to force links update (there is likely a more efficient way)
        self.parameterized.setValue(name, None, isdefault)
        # TODO: if isdefault, we should look if a link can provide a linked value
        # rather than the current one.
        self.parameterized.setValue(name, value, isdefault)
        self.editors[name].valuePropertiesChanged(isdefault)

    def _lock_system(self, name):
        """function for lock system : lock a diskItem if the file exists"""
        # print("-- FUNCTION _lock_system  : neuroProcessesGUI / ParameterizedWidget-- ")
        # value = self.parameterized.__getattribute__(name)
        value = getattr(self.parameterized, name, None)

        if value is not None:
            try:
                isLock = value.lockData()
                if isLock:
                    self.labels[name].lock_id.setChecked(True)
                else:
                    self.labels[name].lock_id.setChecked(False)
            except:
                self.labels[name].lock_id.setChecked(False)
                print('Could not change lock for', name, ':')
                import traceback
                traceback.print_exc()

    def _unlock_system(self, name):
        """function for lock system : unlock a file"""
        # print("-- FUNCTION _unlock_system : neuroProcessesGUI / ParameterizedWidget-- ")
        # value = self.parameterized.__getattribute__(name)
        value = getattr(self.parameterized, name, None)
        if value is not None:
            value.unlockData()

    def _setlock_system(self, name):
        """function for lock system : lock a diskItem if the file exists"""
        # from brainvisa.data.neuroDiskItems import DiskItem
        # print("-- FUNCTION _setlock_system : neuroProcessesGUI / ParameterizedWidget-- ")
        # value = self.parameterized.__getattribute__(name)
        value = getattr(self.parameterized, name, None)

        if value is not None and isinstance(value, DiskItem):
            valueToSet = value.isLockData()
            return valueToSet
        else:
            return (False)

    def checkReadable(self):
        for (n, p) in items(self.parameterized.signature):
            if p.checkReadable(getattr(self.parameterized, n, None)):
                e = self.editors.get(n)
                if e is not None:
                    e.checkReadable()

    def readUserValues(self):
        'Ensure that values typed by the user are taken into account'
        for n in self.parameterized.signature.keys():
            e = self.editors.get(n)
            if e is not None:
                e.checkValue()

    def setValue(self, parameterName, value, default=0):
        'Set the value of a parameter'
        oldValue = self.editors[parameterName].getValue()
        self.editors[parameterName].setValue(value, default=default)
        # use signals-slots to update the parameter label gui if the lock status of the matching value changes.
        # DiskItems now inherit from QObject and emit lockChanged signal when
        # the lock changes
        if isinstance(oldValue, QObject) and hasattr(value, 'lockChanged'):
            try:
                oldValue.lockChanged.disconnect(
                    self.labels[parameterName].setlock)
            except:
                pass
            try:
                oldValue.lockChanged.disconnect(
                    self.editors[parameterName].lockChanged)
            except:
                pass
        if isinstance(value, QObject) and hasattr(value, 'lockChanged'):
            value.lockChanged.connect(self.labels[parameterName].setlock)
            value.lockChanged.connect(self.editors[parameterName].lockChanged)


#----------------------------------------------------------------------------
class BrainVISAAnimation(QLabel):

    def __init__(self, parent=None):
        QLabel.__init__(self, parent)
        self.mmovie = QMovie(
            os.path.join(neuroConfig.iconPath, 'rotatingBrainVISA.gif'))  # , 1024*10 )
        self.setMovie(self.mmovie)
        # self.mmovie.setSpeed( 500 )
        # qApp.processEvents()
        self.mmovie.stop()

    def start(self):
        self.mmovie.start()

    def stop(self):
        self.mmovie.start()
        qApp.processEvents()
        self.mmovie.stop()


#----------------------------------------------------------------------------
class SectionTitle(QLabel):

    """
    This widget is used to create a virtual signature separation, called implicitly by "section" argument.
    All parameters will be grouped by section in signature with this label text as title.

    **Examples**

    'bool_field', Boolean(section='Section title'),
    or
    'image', WriteDiskItem( '4D Volume', 'NIFTI-1 image', section='Output images' ),
    """

    def __init__(self, section_title, section_widgets=[], collapsed=False):
        if collapsed:
            printSectionTitle = '<span style="font-size:smaller;">&#9654;</span> ' + \
                section_title
        else:
            printSectionTitle = '<span style="font-size:smaller;">&#9660;</span> ' + \
                section_title
        QLabel.__init__(self, printSectionTitle)
        font = QFont()
        # font.setCapitalization(QFont.Capitalize)
        # font.setUnderline(True)
        font.setItalic(True)
        self.setFont(font)
        self.setAlignment(Qt.AlignCenter)

        styleSheet = """

SectionTitle { border-top: 1px solid #6c6c6c;
               border-top-style: double;
               border-top-color: silver; }"""
        self.setStyleSheet(styleSheet)

        self.section_title = section_title
        self.section_widgets = [w for w in section_widgets]
        self.collapsed = collapsed

    def mouseReleaseEvent(self, ev):
        self.changeVisibility()

    def changeVisibility(self):
        self.setCollapsed(not self.collapsed)

    def setCollapsed(self, collapsed):
        self.collapsed = collapsed
        if self.collapsed:
            printSectionTitle = '<span style="font-size:smaller;">&#9654;</span> ' + \
                self.section_title
        else:
            printSectionTitle = '<span style="font-size:smaller;">&#9660;</span> ' + \
                self.section_title
        self.setText(printSectionTitle)
        for widget in self.section_widgets:
            if self.collapsed:
                action = 'hide'
                widget.hide()
            else:
                action = 'show'
                widget.show()

    def addSectionWidgets(self, *args):
        for w in args:
            self.section_widgets.append(w)
            if self.collapsed:
                w.hide()
            else:
                w.show()


#----------------------------------------------------------------------------
class ProcessView(QWidget, ExecutionContextGUI):

    # actions:
    action_save_process = None

    action_clone_process = None

    action_create_workflow = None

    action_run = None

    action_interupt = None

    action_interupt_step = None

    action_run_with_sw = None

    action_iterate = None

    action_show_doc = None

    eTreeWidget = None

    menu = None

    parameterizedWidget = None

    read_only = None

    closed = QtCore.Signal()

    def __init__(self,
                 processId,
                 parent=None,
                 externalInfo=None,
                 read_only=False):
        ExecutionContextGUI.__init__(self)
        QWidget.__init__(self, parent)
        if getattr(ProcessView, 'pixIcon', None) is None:
            setattr(ProcessView, 'pixIcon', QIcon(
                os.path.join(neuroConfig.iconPath, 'icon_process.png')))
            setattr(ProcessView, 'pixDefault', QIcon(
                os.path.join(neuroConfig.iconPath, 'lock.png')))
            setattr(ProcessView, 'pixInProcess', QIcon(
                os.path.join(neuroConfig.iconPath, 'forward.png')))
            setattr(ProcessView, 'pixProcessFinished', QIcon(
                os.path.join(neuroConfig.iconPath, 'ok.png')))
            setattr(ProcessView, 'pixProcessError', QIcon(
                os.path.join(neuroConfig.iconPath, 'abort.png')))
            setattr(ProcessView, 'pixNone', QIcon())

        self.read_only = read_only

        # ProcessView cannot be a QMainWindow because it have to be included in a QStackedWidget in pipelines.
        # centralWidget=QWidget()
        # self.setCentralWidget(centralWidget)

        centralWidgetLayout = QVBoxLayout()
        self.setLayout(centralWidgetLayout)
        centralWidgetLayout.setContentsMargins(5, 5, 5, 5)
        centralWidgetLayout.setSpacing(4)

        self.setWindowIcon(self.pixIcon)
        self.workflowEnabled = False

        self.action_save_process = QAction(_t_('&Save...'), self)
        self.action_save_process.setShortcut(Qt.CTRL + Qt.Key_S)
        self.action_save_process.triggered.connect(self.saveAs)

        self.action_clone_process = QAction(_t_('&Clone...'), self)
        self.action_clone_process.setShortcut(Qt.CTRL + Qt.Key_C)
        self.action_clone_process.triggered.connect(self.clone)

        self.action_create_workflow = QAction(_t_('Create &Workflow...'), self)
        self.action_create_workflow.setShortcut(Qt.CTRL + Qt.Key_D)
        self.action_create_workflow.triggered.connect(self.createWorkflow)

        self.action_run = QAction(_t_('Run'), self)
        self.action_run.triggered.connect(self._run)

        self.action_run_with_sw = QAction(_t_('Run in parallel'), self)
        self.action_run_with_sw.setToolTip(
            'Run in parallel using Soma-workflow')
        self.action_run_with_sw.triggered.connect(
            self._run_with_soma_workflow_button)

        self.action_interupt = QAction(_t_('Interrupt'), self)
        self.action_interupt.triggered.connect(self._interruptButton)
        self.action_interupt.setVisible(False)

        self.action_interupt_step = QAction(
            _t_('Interrupt current step'), self)
        self.action_interupt_step.triggered.connect(self._interruptStepButton)
        self.action_interupt_step.setVisible(False)

        self.action_iterate = QAction(_t_('Iterate'), self)
        self.action_iterate.triggered.connect(self._iterateButton)

        self.action_lock_all = QAction(_t_('Lock all files'), self)
        self.action_lock_all.triggered.connect(self.menuLockAllFiles)

        self.action_unlock_all = QAction(_t_('Unlock all files'), self)
        self.action_unlock_all.triggered.connect(self.menuUnlockAllfiles)

        self.action_manage_process_setups = QAction(
            _t_('Save process setups'), self)
        self.action_manage_process_setups.triggered.connect(
            self.saveProcessSetupsGUI)
        self.action_show_doc = QAction(_t_('Show process documentation'), self)
        self.action_show_doc.setShortcut(Qt.Key_F1)
        self.action_show_doc.triggered.connect(self.show_process_doc)

        if parent is None:
            neuroConfig.registerObject(self)
            # menu bar
            self.menu = QMenuBar(self)
            addBrainVISAMenu(self, self.menu)

            # warning: don't create the menu using addMenu() in PyQt
            processMenu = QMenu(_t_("&Process"), self.menu)
            self.menu.addMenu(processMenu)
            processMenu.addAction(self.action_save_process)
            processMenu.addAction(self.action_clone_process)
            processMenu.addAction(self.action_manage_process_setups)
            processMenu.addAction(self.action_iterate)
            _addSeparator(processMenu)
            processMenu.addAction(self.action_create_workflow)
            _addSeparator(processMenu)
            processMenu.addAction(self.action_run)
            processMenu.addAction(self.action_interupt)
            processMenu.addAction(self.action_interupt_step)
            _addSeparator(processMenu)
            processMenu.addAction(self.action_run_with_sw)
            _addSeparator(processMenu)
            processMenu.addAction(self.action_lock_all)
            processMenu.addAction(self.action_unlock_all)

            # warning: don't create the menu using addMenu() in PyQt
            view_menu = QMenu(_t_("&View"), self.menu)
            self.menu.addMenu(view_menu)
            view_menu.addAction(close_viewers_action(self))
            view_menu.addAction(self.action_show_doc)

            try:
                import soma_workflow
                self.workflowEnabled = True
            except ImportError:
                pass
            if not self.workflowEnabled:
                self.action_create_workflow.setEnabled(False)
            centralWidgetLayout.addWidget(self.menu)

        self.destroyed.connect(self.cleanup)

        process = brainvisa.processes.getProcessInstance(processId)
        if process is None:
            raise RuntimeError(neuroException.HTMLMessage(
                _t_('Cannot open process <em>%s</em>') % (str(processId), )))
        self.process = process
        self.process.guiContext = weakref.proxy(self)
        self._runningProcess = 0
        self.process.signatureChangeNotifier.add(self.signatureChanged)
        self.btnRun = None
        self.btnRunSomaWorkflow = None
        self.btnInterrupt = None
        self.btnInterruptStep = None
        self._running = False

        if self.process.__class__ == brainvisa.processes.IterationProcess:
            self.action_iterate.setVisible(False)

        procdoc = brainvisa.processes.readProcdoc(process)
        documentation = procdoc.get(neuroConfig.language)
        if documentation is None:
            documentation = procdoc.get('en', {})

        t = _t_(process.name) + ' ' + unicode(process.instance)
        self.setWindowTitle(t)

        if process.showMaximized:
            self.showMaximized()

        # title of the process : label + rotating icon when it's running
        titleLayout = QHBoxLayout()
        centralWidgetLayout.addLayout(titleLayout)
        if not parent:
            self.labName = QLabel(t, self)
        else:
            self.labName = QLabel(_t_(process.name), self)
        titleLayout.addWidget(self.labName)
        self.labName.setFrameStyle(QFrame.Panel | QFrame.Raised)
        self.labName.setLineWidth(1)
        self.labName.setContentsMargins(5, 5, 5, 5)
        self.labName.setAlignment(Qt.AlignCenter)
        self.labName.setWordWrap(True)
        font = self.labName.font()
        font.setPointSize(QFontInfo(font).pointSize() + 4)
        self.labName.setFont(font)
        self.labName.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred))
        doc = XHTML.html(documentation.get('short', ''))
        if doc:
            self.labName.setToolTip(html_with_local_images(
                '<center><b>' + _t_(process.name) + '</b></center><hr><b>' + _t_('Description') + ':</b><br/>' + doc))

        if externalInfo is None:

            self.movie = BrainVISAAnimation()
            titleLayout.addWidget(self.movie)
            titleLayout.setSpacing(3)

            # vertical splitter : parameters, log text widget
            splitter = QSplitter(Qt.Vertical)
            centralWidgetLayout.addWidget(splitter)

            # at the top of the splitter : the parameters
            self.parametersWidget = QWidget(splitter)
            parametersWidgetLayout = QVBoxLayout()
            parametersWidgetLayout.setContentsMargins(0, 0, 0, 0)
            self.parametersWidget.setLayout(parametersWidgetLayout)

            # at the bottom of the splitter : the text widget to log
            # information about process execution
            self.info = QTextEdit(splitter)
            self.info.setReadOnly(True)
            self.info.setAcceptRichText(True)
            sizePolicy = QSizePolicy(
                QSizePolicy.Expanding, QSizePolicy.Preferred)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(50)
            self.info.setSizePolicy(sizePolicy)
            self.infoCounter = None
            # splitter.setResizeMode( self.info, splitter.Stretch )
            # splitter.setResizeMode( self.parametersWidget, QSplitter.FollowSizeHint )
            # splitter.setResizeMode( self.parametersWidget, QSplitter.Auto )
            container = self.parametersWidget
            self.isMainWindow = True
        else:
            self.movie = None
            splitter = None
            self.parametersWidget = self
            container = self
            self.isMainWindow = False
            self.info = externalInfo

        self.splitter = splitter
        self._widgetStack = None
        eNode = getattr(process, '_executionNode', None)
        self._executionNodeLVItems = {}
        # process composed of several processes
        if eNode is not None and self.isMainWindow:
            self.parameterizedWidget = None
            # vb = QVBoxLayout( )
            # container.layout().addLayout(vb)
            vb = container.layout()

            # splitter that shows the composition of the process on the left
            # and the parameters of each step on the right
            self.eTreeWidget = QSplitter(Qt.Horizontal)
            vb.addWidget(self.eTreeWidget)

            # Run and iterate buttons
            self.inlineGUI = self.process.inlineGUI(self.process, self, None,
                                                    externalRunButton=True)
            if self.inlineGUI is None and externalInfo is None:
                self.inlineGUI = self.defaultInlineGUI(None)
            vb.addWidget(self.inlineGUI)

            # composition of the pipeline
            self.executionTree = QTreeWidget(self.eTreeWidget)
            self.executionTree.setSizePolicy(
                QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred))
            self.executionTree.setColumnCount(1)
            self.executionTree.setHeaderLabels(['Name'])
            self.executionTree.setAllColumnsShowFocus(1)
            self.executionTree.setRootIsDecorated(1)
            self.executionTree.setContextMenuPolicy(Qt.CustomContextMenu)
            # Popup Menu for toolboxes
            self.executionTreeMenu = QMenu(self)
            _addAction(self.executionTreeMenu, _t_(
                "Unselect before"), self.menuUnselectBefore)
            _addAction(self.executionTreeMenu, _t_(
                "Unselect after"), self.menuUnselectAfter)
            # self.executionTreeMenu.addAction( _t_("Unselect all"),
            # self.menuUnselectAll )
            _addAction(self.executionTreeMenu, _t_(
                "Select before"), self.menuSelectBefore)
            _addAction(self.executionTreeMenu, _t_(
                "Select after"), self.menuSelectAfter)
            # self.executionTreeMenu.addAction( _t_("Select all"),
            # self.menuSelectAll )
            _addAction(self.executionTreeMenu)
            _addAction(self.executionTreeMenu, _t_(
                "Unselect steps writing locked files"), self.menuUnselectLocked)
            _addAction(self.executionTreeMenu, _t_(
                "Unselect steps upstream of locked files"), self.menuUnselectLockedUpstream)
            _addAction(self.executionTreeMenu)
            self.executionTreeMenu._opennodeaction = _addAction(
                self.executionTreeMenu, _t_("Open this step separately"),
              self.menuDetachExecutionNode)
            self.executionTreeMenu._showdocaction \
                = _addAction(self.executionTreeMenu, _t_("Show documentation"),
                             self.menuShowDocumentation)

            _addAction(self.executionTreeMenu)
            _addAction(
                self.executionTreeMenu, _t_("Lock files under this node"),
                       self.menuLockStep)
            _addAction(
                self.executionTreeMenu, _t_("Unlock files under this node"),
                       self.menuUnlockStep)

            self.executionTreeMenu._nodeactionseparator \
                = _addAction(self.executionTreeMenu)
            self.executionTreeMenu._addnodeaction \
                = _addAction(self.executionTreeMenu, _t_("Add node"),
                             self.menuAddExecutionNode)
            self.executionTreeMenu._removenodeaction \
                = _addAction(self.executionTreeMenu, _t_("Remove node"),
                             self.menuRemoveExecutionNode)

            self.executionTree.customContextMenuRequested.connect(
                self.openContextMenu)
            # self.executionTree.setSortingEnabled( -1 )
            # self.eTreeWidget.setResizeMode( self.executionTree,
            # QSplitter.KeepSize )

            if self.read_only:
                self.executionTreeMenu.setEnabled(False)

            # parameters of a each step of the pipeline
            self._widgetStack = QStackedWidget(self.eTreeWidget)
            self._widgetStack.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,
                                                        QSizePolicy.Preferred))
            self._widgetStack._children = []

            # set splitter sizes to avoid the widget stack to be hidden in case
            # it is currently empty
            self.eTreeWidget.setSizes([150, 250])

            self._guiId = 0
            self._executionNodeExpanded(self.executionTree, (eNode, (eNode,)))
            self.executionTree.itemExpanded.connect(
                self._executionNodeExpanded)

            self.executionTree.currentItemChanged.connect(
                self.executionNodeSelected)
            # don't listen to clicks, but to item changes instead
            # because stats check can be triggered via the keyboard, not only
            # mouse clicks
            # self.executionTree.itemClicked.connect(self.executionNodeClicked)
            self.executionTree.itemChanged.connect(self.executionNodeChanged)

            # Select and open the first item
            item = self.executionTree.topLevelItem(0)
            if item is not None:
                item.setExpanded(True)
                self.executionTree.setCurrentItem(item)

        # simple process : only a signature, no sub-processes
        else:
            self.executionTree = None
            self.createSignatureWidgets(documentation)

        self._iterationDialog = None

        self._logDialog = None
        # It is necessary to call show() before resize() to have a correct layout.
        # Otherwize,  vertical sliders may be superimposed to widgets. But some
        # window managers compute the window location according to its size during
        # show(). If the window become larger after show() call, part of it will be
        # off screen. Therefore set the widget size before show() ensure that
        # window location is correct and changing size after show() ensure that
        # layout is correct.
        if parent is None:
            self.show()
            self.resize(600, 801)
            # qApp.processEvents()

        initGUI = getattr(self.process, 'initializationGUI', None)
        if initGUI is not None:
            initGUI(self)

        if self.read_only and self.parameterizedWidget != None:
            self.parameterizedWidget.set_read_only(True)

    def createSignatureWidgets(self, documentation=None):
        eNode = getattr(self.process, '_executionNode', None)
        signatureWidget = None
        # if the process has a signature, creates a widget for the parameters :
        # ParameterizedWidget
        if eNode and self.isMainWindow:
            parent = self._widgetStack
            if self.process.signature:
                signatureWidget = eNode.gui(parent, processView=self)
        else:
            parent = self.parametersWidget.layout()
            if self.process.signature:
                signatureWidget = ParameterizedWidget(self.process, None)
                parent.addWidget(signatureWidget)
            self.parameterizedWidget = signatureWidget

        if eNode is None or not self.isMainWindow:
            self.inlineGUI = self.process.inlineGUI(self.process, self, None)
            if self.inlineGUI is None and self.isMainWindow:
                self.inlineGUI = self.defaultInlineGUI(None)
            if self.inlineGUI is not None:
                parent.addWidget(self.inlineGUI)
        else:
            self._widgetStack.removeWidget(self._widgetStack._children[0])
            self._widgetStack._children[0].close()
            self._widgetStack._children[0].deleteLater()
            if signatureWidget is not None:
                self._widgetStack.insertWidget(0, signatureWidget)
            self._widgetStack._children[0] = signatureWidget
            self._widgetStack.setCurrentIndex(0)
#    if self.parameterizedWidget is not None:
#      if documentation is not None:
#        for ( k, p ) in items(self.process.signature):
#          if neuroConfig.userLevel >= p.userLevel:
#            self.parameterizedWidget.setParameterToolTip( k,
#              XHTML.html( documentation.get( 'parameters', {} ).get( k, '' ) ) \
#              + '<br/><img src="' \
#              + os.path.join( neuroConfig.iconPath, 'lock.png' )+ '"/><em>: ' \
#              + _t_( \
#              'value has been manually changed and is not linked anymore' ) \
#              + '</em>' )
#      self.parameterizedWidget.show()
#    if self.inlineGUI is not None:
#      self.inlineGUI.show()

        if self.parameterizedWidget != None:
            self.parameterizedWidget.set_read_only(self.read_only)

    def eraseSignatureWidgets(self):
        if self.parameterizedWidget is not None:
            self.parameterizedWidget.close()
            self.parameterizedWidget.deleteLater()
        eNode = getattr(self.process, '_executionNode', None)
        if eNode is None and self.inlineGUI is not None:
            self.inlineGUI.close()
            self.inlineGUI.deleteLater()

    def signatureChanged(self, process):
        self.eraseSignatureWidgets()
        self.createSignatureWidgets(None)

    # Execution tree menu
    def openContextMenu(self, point):
        """
        Called on contextMenuRequested signal. It opens the popup menu at cursor position.
        """
        item = self.executionTree.currentItem()
        if item:
            enode = item._executionNode
            parent = item.parent()
            if parent:
                pnode = parent._executionNode
            else:
                pnode = None
            if hasattr(enode, '_process'):
                self.executionTreeMenu._showdocaction.setEnabled(True)
            else:
                self.executionTreeMenu._showdocaction.setEnabled(False)

            # Show/Hide node actions
            if isinstance(enode, brainvisa.processes.ProcessExecutionNode):
                enode = enode._executionNode

            if isinstance( enode, brainvisa.processes.ParallelExecutionNode ) \
               and enode.possibleChildrenProcesses:
                self.executionTreeMenu._addnodeaction.setVisible(True)
            else:
                self.executionTreeMenu._addnodeaction.setVisible(False)

            if isinstance(pnode, brainvisa.processes.ProcessExecutionNode):
                pnode = pnode._executionNode

            if isinstance( pnode, brainvisa.processes.ParallelExecutionNode ) \
               and pnode.possibleChildrenProcesses:
                self.executionTreeMenu._removenodeaction.setVisible(True)
            else:
                self.executionTreeMenu._removenodeaction.setVisible(False)

            self.executionTreeMenu._nodeactionseparator.setVisible(
                self.executionTreeMenu._addnodeaction.isVisible() or
                self.executionTreeMenu._removenodeaction.isVisible())

            self.executionTreeMenu.exec_(QCursor.pos())

    def changeItemSelection(self, select=True, all=True, before=False):
        item = self.executionTree.currentItem()
        if item:
            parent = item.parent()
            if parent:
                if all:
                    r = xrange(parent.childCount())
                elif before:
                    r = xrange(parent.indexOfChild(item))
                else:  # after
                    r = xrange(
                        parent.indexOfChild(item) + 1, parent.childCount())
                for i in r:
                    parent.child(i).check(select)
            else:
                parent = item.treeWidget()
                if all:
                    r = xrange(parent.topLevelItemCount())
                elif before:
                    r = xrange(parent.indexOfTopLevelItem(item))
                else:  # after
                    r = xrange(parent.indexOfTopLevelItem(
                        item) + 1, parent.topLevelItemCount())
                for i in r:
                    parent.topLevelItem(i).check(select)

    def menuUnselectBefore(self):
        self.changeItemSelection(select=False, all=False, before=True)

    def menuUnselectAfter(self):
        self.changeItemSelection(select=False, all=False, before=False)

    # def menuUnselectAll(self):
        # self.changeItemSelection(select=False, all=True, before=False)

    def menuSelectBefore(self):
        self.changeItemSelection(select=True, all=False, before=True)

    def menuSelectAfter(self):
        self.changeItemSelection(select=True, all=False, before=False)

    # def menuSelectAll(self):
        # self.changeItemSelection(select=True, all=True, before=False)

    def lockedSteps(self, useUnselected=False):
        items = [self.process.executionNode()]
        locked = []
        while items:
            item = items.pop()
            if not useUnselected and not item.isSelected():
                continue
            children = list(item.children())
            if len(children) == 0:  # terminal node
                process = item._process
                for n, p in six.iteritems(process.signature):
                    if isinstance(p, WriteDiskItem):
                        v = getattr(item._process, n)
                        # test if data is locked
                        if v is not None and v.isLockData():
                            locked.append(item)
            else:
                if useUnselected:
                    items += list(item.children())
                else:
                    items += [x for x in item.children() if x.isSelected()]
        return locked

    def parentNodes(self, enode):
        items = [[self.process.executionNode()]]
        chain = []
        # walk the tree "vertically" getting deep in branches first
        while items:
            iteml = items[-1]
            if len(iteml) == 0:
                items.pop()
                chain.pop()
                continue
            item = iteml.pop()
            if item is enode:
                return chain
            children = list(item.children())
            if len(children) != 0:
                chain.append(item)
                items.append(list(item.children()))

    def menuUnselectLocked(self):
        locked = self.lockedSteps()
        for item in locked:
            if item._optional:
                item.setSelected(False)
            else:
                parents = [x for x in self.parentNodes(item) if x._optional]
                if parents:
                    parents[-1].setSelected(False)

    def menuUnselectLockedUpstream(self):
        items = self.lockedSteps(useUnselected=True)
        while items:
            item = items.pop()
            if item._optional:
                item.setSelected(False)
            else:
                parents = [x for x in self.parentNodes(item) if x._optional]
                if parents:
                    parents[-1].setSelected(False)
                    items.append(parents[-1])
                    continue
            parents = self.parentNodes(item)
            parents.reverse()
            for p in parents:
                if p.isSelected():
                    if p.__class__ is brainvisa.processes.SerialExecutionNode:
                        for sp in p.children():
                            if sp is item:
                                break
                            if sp._optional:
                                sp.setSelected(False)
                            else:
                                sparents = [
                                    x for x in self.parentNodes(sp) if x._optional]
                                if sparents:
                                    sparents[-1].setSelected(False)
                    item = p

    def _changeAllLockedFiles(self, procOrNode, setLock):
        files = procOrNode.allParameterFiles()
        # filter out non-existing files and already locked ones
        if setLock:
            files = [
                f for f in files if f.isWriteable() and not f.isLockData()]
        else:
            files = [f for f in files if f.isWriteable() and f.isLockData()]
        # show and confirm
        dialog = lockFilesGUI.LockedFilesListEditor(self, files, setLock)
        if dialog.exec_():
            files = dialog.selectedDiskItems()
            if files:
                if setLock:
                    print('Locking...')
                    for f in files:
                        try:
                            f.lockData()
                        except IOError:
                            pass  # probably not writeable
                    print('done.')
                else:
                    print('Unlocking...')
                    for f in files:
                        try:
                            f.unlockData()
                        except IOError:
                            pass
                    print('done.')

    def menuLockAllFiles(self):
        self._changeAllLockedFiles(self.process, True)

    def menuUnlockAllfiles(self):
        self._changeAllLockedFiles(self.process, False)

    def menuDetachExecutionNode(self):
        item = self.executionTree.currentItem()
        if item:
            proc = item._executionNode
            self.readUserValues()
            event = ProcessExecutionEvent()
            event.setProcess(brainvisa.processes.getProcessInstance(proc))
            clone = brainvisa.processes.getProcessInstanceFromProcessEvent(
                event)
            return showProcess(clone)

    def menuShowDocumentation(self):
        global _mainWindow
        item = self.executionTree.currentItem()
        if item:
            enode = item._executionNode
            if hasattr(enode, '_process'):
                proc = enode._process
                doc = brainvisa.processes.getHTMLFileName(proc)
                if os.path.exists(doc):
                    _mainWindow.info.setSource(doc)

    def menuLockStep(self):
        global _mainWindow
        item = self.executionTree.currentItem()
        if item:
            enode = item._executionNode
            self._changeAllLockedFiles(enode, True)

    def menuUnlockStep(self):
        global _mainWindow
        item = self.executionTree.currentItem()
        if item:
            enode = item._executionNode
            self._changeAllLockedFiles(enode, False)

    def menuAddExecutionNode(self):
        item = self.executionTree.currentItem()
        if item:
            enode = item._executionNode

            if isinstance(enode, brainvisa.processes.ProcessExecutionNode):
                enode = enode._executionNode

            if isinstance( enode, brainvisa.processes.SerialExecutionNode ) \
               and enode.possibleChildrenProcesses:
                defaultProcess = enode.possibleChildrenProcesses.keys()[0]
                defaultProcessOptions = enode.possibleChildrenProcesses[
                    defaultProcess]
                child = brainvisa.processes.ProcessExecutionNode(
                    defaultProcess,
                          optional=defaultProcessOptions.get(
                              'optional', True),
                          selected=defaultProcessOptions.get(
                              'selected', True),
                          expandedInGui=defaultProcessOptions.get(
                              'expandedInGui', False)
                )
                enode.addChild(node=child)

    def menuRemoveExecutionNode(self):
        item = self.executionTree.currentItem()
        parent = item.parent()
        if parent:
            pnode = parent._executionNode
            if isinstance(pnode, brainvisa.processes.ProcessExecutionNode):
                pnode = pnode._executionNode
            if isinstance( pnode, brainvisa.processes.SerialExecutionNode ) \
                    and pnode.possibleChildrenProcesses:
                n = pnode.childrenNames()
                for k in n:
                    c = pnode._children[k]
                    if c and ((c is item._executionNode)
                              or (weakref.proxy(c) is item._executionNode)):
                        pnode.removeChild(k)

    def defaultInlineGUI(self, parent, externalRunButton=False, container=None):
        if container is None:
            container = QWidget()
            layout = QHBoxLayout()
            container.setLayout(layout)
            layout.setContentsMargins(5, 5, 5, 5)
        else:
            layout = container.layout()

        if not externalRunButton:
            self.btnRun = QToolButton(self)
            self.btnRun.setDefaultAction(self.action_run)
            self.btnRun.setToolButtonStyle(Qt.ToolButtonTextOnly)
            layout.addWidget(self.btnRun)
            self.btnRun.setMinimumWidth(90)
            self.btnRun.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))

            self.btnInterrupt = QToolButton(self)
            self.btnInterrupt.setDefaultAction(self.action_interupt)
            self.btnInterrupt.setToolButtonStyle(Qt.ToolButtonTextOnly)
            layout.addWidget(self.btnInterrupt)
            self.btnInterrupt.setMinimumWidth(90)
            self.btnInterrupt.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            self.btnInterrupt.setVisible(False)

            self.btnInterruptStep = QToolButton(self)
            self.btnInterruptStep.setDefaultAction(self.action_interupt_step)
            self.btnInterruptStep.setToolButtonStyle(Qt.ToolButtonTextOnly)
            layout.addWidget(self.btnInterruptStep)
            self.btnInterruptStep.setMinimumWidth(90)
            self.btnInterruptStep.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            self.btnInterruptStep.setVisible(False)

        self.btnIterate = QToolButton(self)
        self.btnIterate.setDefaultAction(self.action_iterate)
        self.btnIterate.setToolButtonStyle(Qt.ToolButtonTextOnly)
        layout.addWidget(self.btnIterate)
        self.btnIterate.setMinimumWidth(90)
        self.btnIterate.setSizePolicy(
            QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        if self.process.__class__ == brainvisa.processes.IterationProcess:
            self.btnIterate.setVisible(False)

        if self.process.executionNode() != None and _workflow_application_model != None:
            self.btnRunSomaWorkflow = QToolButton(self)
            self.btnRunSomaWorkflow.setDefaultAction(self.action_run_with_sw)
            self.btnRunSomaWorkflow.setToolButtonStyle(Qt.ToolButtonTextOnly)
            layout.addWidget(self.btnRunSomaWorkflow)
            self.btnRunSomaWorkflow.setMinimumWidth(90)
            self.btnRunSomaWorkflow.setSizePolicy(
                QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        else:
            self.action_run_with_sw.setVisible(False)

        container.setSizePolicy(
            QSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed))
        container.setMaximumHeight(container.sizeHint().height())
        return container

    def getEditor(self, parameterName):
        if self.parameterizedWidget is not None:
            return self.parameterizedWidget.editors.get(parameterName)
        return None

    def closeEvent(self, event):
        self.cleanup()
        self.closed.emit()
        QWidget.closeEvent(self, event)

    def cleanup(self):
        self.process.cleanup()
        if self.parameterizedWidget is not None:
            self.parameterizedWidget.cleanup()
        if self._widgetStack is not None:
            for gui in self._widgetStack._children:
                cleanup = getattr(gui, 'cleanup', None)
                if cleanup is not None:
                    cleanup()
                if gui is not None and sip is not None and not sip.isdeleted(gui):
                    gui.deleteLater()
            self._widgetStack = None
        if self.executionTree is not None and QTreeWidgetItemIterator:
            it = QTreeWidgetItemIterator(self.executionTree)
            while it.value():
                cleanup = getattr(it.value(), 'cleanup', None)
                if cleanup is not None:
                    cleanup()
                it += 1
            self.executionTree = None
        if neuroConfig:
            neuroConfig.unregisterObject(self)
        self.process.signatureChangeNotifier.remove(self.signatureChanged)
        self._executionNodeLVItems.clear()
        self.parametersWidget = None
        self.info = None
        self.process._lastResult = None

    def _run(self):
        self._runButton()

    def _runButton(self, executionFunction=None):
        try:
            try:
                # disable run button when clicked to avoid several successive clicks
                # it is enabled when the process starts, the label of the
                # button switch to interrupt
                self.action_run.setEnabled(False)
                processView = self._checkReloadProcess()
                if processView is None:
                    processView = self
                    processView.info.setText('')
                else:
                    processView.info.setText('')
                    processView.warning(
                        _t_('processes %s updated') % _t_(processView.process.name))
                if hasattr(self.process, 'executionWorkflow'):
                    return self._run_with_soma_workflow(
                        executionFunction=executionFunction)
                else:
                    processView._runningProcess = 0
                    processView._startCurrentProcess(executionFunction)
            except:
                neuroException.showException()
        finally:
            self.action_run.setEnabled(True)

    def _run_with_soma_workflow(self, executionFunction=None):
        from brainvisa.workflow import ProcessToSomaWorkflow

        submission_dlg = WorkflowSubmissionDlg(self)
        if submission_dlg.exec_() != QtGui.QDialog.Accepted:
            return

        resource_id = submission_dlg.combo_resource.currentText()
        if resource_id != _workflow_application_model.current_resource_id:
            _workflow_application_model.set_current_connection(resource_id)

        qtdt = submission_dlg.dateTimeEdit_expiration.dateTime()
        date = datetime(
            qtdt.date().year(), qtdt.date().month(), qtdt.date().day(),
                        qtdt.time().hour(), qtdt.time().minute(), qtdt.time().second())
        if sys.version_info[0] >= 3:
            queue = submission_dlg.combo_queue.currentText()
        else:
            queue = unicode(
                submission_dlg.combo_queue.currentText()).encode('utf-8')
        if queue == "default queue":
            queue = None

        input_file_processing = submission_dlg.combo_in_files.currentText()
        output_file_processing = submission_dlg.combo_out_files.currentText()

        brainvisa_cmd = [os.path.basename(sys.executable),
                         '-m', 'brainvisa.axon.runprocess']

        self.readUserValues()

        builtin_db = []
        if input_file_processing == ProcessToSomaWorkflow.BV_DB_SHARED_PATH:
            for db_setting in neuroConfig.dataPath:
                if db_setting.builtin:
                    uuid = db_setting.expert_settings.uuid
                    if uuid:
                        builtin_db.append(uuid)
                    else:
                        print("warning ! db " + repr(
                            db_setting.directory) + " has no uuid.")

        ptowf = ProcessToSomaWorkflow(
            self.process,
          input_file_processing=input_file_processing,
          output_file_processing=output_file_processing,
          brainvisa_cmd=brainvisa_cmd,
          brainvisa_db=builtin_db,
          context=self)
        workflow = ptowf.doIt()

        name = unicode(submission_dlg.lineedit_wf_name.text())
        if name == "":
            if workflow.name != None:
                name = SomaWorkflowWidget.brainvisa_code + workflow.name
            else:
                name = SomaWorkflowWidget.brainvisa_code
        else:
            name = SomaWorkflowWidget.brainvisa_code + name

        # store the process in workflow.user_storage
        serialized_process = StringIO()
        event = self.createProcessExecutionEvent()
        event.save(serialized_process)

        to_store = [
            SomaWorkflowWidget.brainvisa_code, serialized_process.getvalue()]
        workflow.user_storage = to_store

        serialized_process.close()

        _workflow_application_model.add_workflow(
            soma_workflow.gui.workflowGui.NOT_SUBMITTED_WF_ID,
                              datetime.now() + timedelta(days=5),
                              name,
                              soma_workflow.constants.WORKFLOW_NOT_STARTED,
                              workflow)

        (wf_id, resource_id) = _mainWindow.sw_widget.submit_workflow(date,
                                                                     name,
                                                                     queue)
        if wf_id == None:
            return

        view = SomaWorkflowProcessView(
            _workflow_application_model,
                              wf_id,
                              resource_id,
                              process=self.process,
                              parent=_mainWindow)
        view.setAttribute(QtCore.Qt.WA_DeleteOnClose)
        view.show()

    def _run_with_soma_workflow_button(self, executionFunction=None):
        try:
            self._run_with_soma_workflow(executionFunction=executionFunction)
        except:
            neuroException.showException()
        finally:
            self.action_run_with_sw.setEnabled(True)

    def _interruptButton(self):
        if self._running:
            try:
                self._setInterruptionRequest(
                    brainvisa.processes.ExecutionContext.UserInterruption())
            except:
                neuroException.showException()

    def _interruptStepButton(self, executionFunction=None):
        if self._running:
            self._setInterruptionRequest(
                brainvisa.processes.ExecutionContext.UserInterruptionStep())

    def _checkReloadProcess(self):
        self.readUserValues()
        reload = False
        for p in self.process.allProcesses():
            pp = brainvisa.processes.getProcess(p)
            if pp is not None and pp is not p and pp is not p.__class__:
                reload = True
                break
        result = None
        if reload:
            eNode = getattr(self.process, '_executionNode', None)
            if eNode is None:
                # Get current process arguments values
                event = ProcessExecutionEvent()
                event.setProcess(self.process)
                # Forget about old process
                self.process.signatureChangeNotifier.remove(
                    self.signatureChanged)
                self.eraseSignatureWidgets()
                # Care about new process
                self.process = brainvisa.processes.getProcessInstanceFromProcessEvent(
                    event)
                self.process.signatureChangeNotifier.add(
                    self.signatureChanged)
                procdoc = brainvisa.processes.readProcdoc(self.process)
                documentation = procdoc.get(neuroConfig.language)
                if documentation is None:
                    documentation = procdoc.get('en', {})
                self.createSignatureWidgets(documentation)
                result = self
            else:
                result = self.clone()
                self.deleteLater()
        return result

    def _startCurrentProcess(self, executionFunction):
        # Remove icon from all ListView items
        for item in self._executionNodeLVItems.values():
            item.setIcon(0, self.pixNone)
        self._lastProcessRaisedException = None
        try:
            self._startProcess(self.process, executionFunction)
            self._running = True
        except Exception as e:
            self._lastProcessRaisedException = e
            neuroException.showException()

    def _write(self, html):
        mainThreadActions().push(self._appendInfo, html)

    def _appendInfo(self, msg):
        # the tags font are here just to avoid display problems in QTextEdit
        # with non html content (color staying red after an error for example)
        if not self.info is None:
            self.info.append("<font>" + msg + "</font>")

    def _processStarted(self):
        if self._depth() == 1:
            if self.movie is not None:
                mainThreadActions().push(self.movie.start)

            mainThreadActions().push(self.action_run.setEnabled, False)
            mainThreadActions().push(self.action_interupt.setVisible, True)
            if self.process.__class__ == brainvisa.processes.IterationProcess:
                mainThreadActions().push(
                    self.action_interupt_step.setVisible, True)

            if self.btnRun != None:
                mainThreadActions().push(self.btnRun.setVisible, False)
                mainThreadActions().push(self.btnInterrupt.setVisible, True)
                if self.process.__class__ == brainvisa.processes.IterationProcess:
                    mainThreadActions().push(
                        self.btnInterruptStep.setVisible, True)

        # Adds an icon on the ListViewItem corresponding to the current process
        # if any
        p = self._currentProcess()
        eNodeItem = self._executionNodeLVItems.get(p)
        if eNodeItem is not None:
            mainThreadActions().push(eNodeItem.setIcon, 0, self.pixInProcess)

        ExecutionContextGUI._processStarted(self)

    def _processFinished(self, result):
        self.process._lastResult = result
        ExecutionContextGUI._processFinished(self, result)

        # Remove icon from the ListViewItem corresponding to the current process
        # if any
        p = self._currentProcess()
        eNodeItem = self._executionNodeLVItems.get(p)
        if eNodeItem is not None:
            if self._lastProcessRaisedException:
                mainThreadActions().push(
                    eNodeItem.setIcon, 0, self.pixProcessError)
            else:
                mainThreadActions().push(eNodeItem.setIcon, 0,
                                         self.pixProcessFinished)

        if self._depth() == 1:
            if self.movie is not None:
                mainThreadActions().push(self.movie.stop)

            mainThreadActions().push(self.action_run.setEnabled, True)
            mainThreadActions().push(self.action_interupt.setVisible, False)
            if self.process.__class__ == brainvisa.processes.IterationProcess:
                mainThreadActions().push(
                    self.action_interupt_step.setVisible, False)

            if self.btnRun != None:
                mainThreadActions().push(self.btnRun.setVisible, True)
                mainThreadActions().push(self.btnInterrupt.setVisible, False)
                if self.process.__class__ == brainvisa.processes.IterationProcess:
                    mainThreadActions().push(
                        self.btnInterruptStep.setVisible, False)

            mainThreadActions().push(self._checkReadable)
            self._running = False
        else:
            mainThreadActions().push(self._checkReadable)

    def system(self, *args, **kwargs):
        ret = ExecutionContextGUI.system(self, *args, **kwargs)
        mainThreadActions().push(self._checkReadable)
        return ret

    def _checkReadable(self):
        if self.parameterizedWidget is not None:
            self.parameterizedWidget.checkReadable()
        if self.executionTree is not None:
            for gui in self._widgetStack._children:
                checkReadable = getattr(gui, '_checkReadable', None)
                if checkReadable is not None:
                    checkReadable()

    def dialog(self, modal, message, signature, *buttons):
        return mainThreadActions().call(UserDialog, self, modal,
                                        message, signature, buttons)

    # def showCallScript( self ):
        # text = "defaultContext().runProcess( '" + self.process.id() + "'"
        # text2 = ''
        # for n in self.process.signature.keys():
            # if not self.process.isDefault( n ):
                # text2 += '  ' + n + ' = ' + repr( self.editors[ n ].getValue() ) + ',\n'
        # if text2:
            # text += ',\n' + text2
        # text += ')\n'

        # txt = TextEditor( text )
        # txt.resize( 800, 600 )
        # txt.show()

    def executionNodeRemoveChild(self, item, eNode, key=None, childNode=None):

        childItem = getattr(childNode, '_guiItem', None)
        if childItem:

            # Remove matching child item
            for i in xrange(item.childCount()):
                c = item.child(i)
                if (childItem == c) or (childItem is weakref.proxy(c)):
                    item.takeChild(i)
                    break

        func = getattr(item, 'hasChildren', None)
        # Update children indicator for the current item
        if func and func():
            item.setChildIndicatorPolicy(item.DontShowIndicator)

    def executionNodeAddChild(self, item, eNode, key=None, childNode=None,
                              previous=None):
        # 10-02-2014 : added possibility to hide nodes in GUI
        if isinstance(item, QTreeWidgetItem) and not item.isExpanded():
            item.setChildIndicatorPolicy(item.ShowIndicator)
            return item
        newItem = None
        oldItem = getattr(childNode, '_guiItem', None)
        if oldItem:
            # WARNING: an execution node has *one* _guiItem, so the current design
            # only allows to have one widget view to the process tree. Thus this
            # forbids displaying in a workflow window the pipeline if it is already
            # displayed in a ProcessView. Even more problematic, it prevents
            # displaying in a workflow window if it happened to have a GUI once in
            # the past, since _guiItem is not cleared when the widget is
            # closed.
            oldItem.setHidden(getattr(childNode, '_hidden', False))
            newItem = oldItem
        else:
            if isinstance(childNode, brainvisa.processes.ProcessExecutionNode):
                en = childNode._executionNode
                if en is None:
                    en = childNode
            else:
                en = childNode
            if eNode is not childNode \
                and (isinstance(eNode, brainvisa.processes.SelectionExecutionNode)
                     or (isinstance(eNode, brainvisa.processes.ProcessExecutionNode)
                         and isinstance(eNode._executionNode, brainvisa.processes.SelectionExecutionNode))):
                itemType = "radio"
            elif childNode._optional:
                itemType = "check"
            else:
                itemType = None

            # Try to insert node at the matching index
            if key:
                index = eNode._children.index(key)
            else:
                index = None

            name = childNode.name()
            newItem = NodeCheckListItem(childNode, item, index, _t_(name),
                                        itemType, read_only=self.read_only)
            newItem.setHidden(getattr(childNode, '_hidden', False))
            if isinstance(childNode, weakref.ProxyType):
                newItem._executionNode = childNode
            else:
                newItem._executionNode = weakref.proxy(childNode)
            if isinstance(newItem, weakref.ProxyType):
                childNode._guiItem = newItem
            else:
                childNode._guiItem = weakref.proxy(newItem)
            # Add callback to warn about child add and remove
            beforeChildRemovedCallback = getattr(
                en, 'beforeChildRemoved', None)
            if beforeChildRemovedCallback:
                beforeChildRemovedCallback.add(
                    soma.functiontools.partial(
                        self.__class__.executionNodeRemoveChild, weakref.proxy(
                            self),
                        newItem))

            afterChildAddedCallback = getattr(en, 'afterChildAdded', None)
            if afterChildAddedCallback:
                afterChildAddedCallback.add(
                    soma.functiontools.partial(
                        self.__class__.executionNodeAddChild, weakref.proxy(
                            self),
                        newItem))

            # set children indicator for the new item
            hiddens = [getattr(c, '_hidden', False) for c in en.children()]
            hasvisiblechildren = (len([x for x in hiddens if not x]) > 0)
            if en.hasChildren() and hasvisiblechildren:
                newItem.setChildIndicatorPolicy(newItem.ShowIndicator)
            # newItem.setExpandable( en.hasChildren() )
            if isinstance(childNode, brainvisa.processes.ProcessExecutionNode):
                self._executionNodeLVItems[childNode._process] = newItem

            # Update children indicator for the current item
            hiddens = [getattr(c, '_hidden', False)
                       for c in eNode.children()]
            hasvisiblechildren = (len([x for x in hiddens if not x]) > 0)
            func = getattr(item, 'hasChildren', None)
            if func and func() \
                    and hasvisiblechildren:
                item.setChildIndicatorPolicy(item.ShowIndicator)

        if self._depth():
            p = self._currentProcess()
            eNodeItem = self._executionNodeLVItems.get(p)
            if eNodeItem is not None:
                eNodeItem.setIcon(0, self.pixInProcess)

        return newItem

    def executionNodeSelected(self, item, previous):
        if item is not None:
            if (getattr(item, "_guiId", None)) is not None:
                self._widgetStack.setCurrentIndex(item._guiId)
            else:
                gui = item._executionNode.gui(
                    self._widgetStack, processView=self)
                if gui is not None:
                    item._guiId = self._widgetStack.addWidget(gui)
                    self._widgetStack._children.append(gui)
                    self._guiId += 1
                else:
                    self._emptyWidget = QWidget(self._widgetStack)
                    item._guiId = self._widgetStack.addWidget(
                        self._emptyWidget)
                self._widgetStack.setCurrentIndex(item._guiId)
            item.currentItemChanged(True)
        if previous is not None:
            previous.currentItemChanged(False)

            # Trick to have correct slider
#      size = self.size()
            # self.resize( size.width()+1, size.height() )
            # qApp.processEvents()
            # self.resize( size )

    # def executionNodeClicked( self, item, column ):
        # item.itemClicked()

    def executionNodeChanged(self, item, column):
        if item._node._selected != item.isOn() \
                and (not hasattr(item, 'creating') or not item.creating):
            item.itemClicked()

    def _executionNodeExpanded(self, item, eNodeAndChildren=None):
        if item is not None and getattr(item, '_notExpandedYet', True):
            item._notExpandedYet = False
            previous = None

            if eNodeAndChildren is None:
                eNode = item._executionNode
                eNodeChildren = (eNode.child(k)
                                 for k in eNode.childrenNames())
            else:
                eNode, eNodeChildren = eNodeAndChildren

            for childNode in eNodeChildren:
                previous = self.executionNodeAddChild(item, eNode,
                                                      childNode=childNode)
                if childNode._expandedInGui and previous is not None:
                    previous.setExpanded(True)

    def _executionNodeActivated(self, item):
        if getattr(item, "activate", None):
            item.activate()

    def createWorkflow(self):
        from brainvisa.workflow import ProcessToSomaWorkflow

        class Options(HasSignature):
            signature = SomaSignature(
                'output',
              SomaFileName,
              dict(doc='Name of the output workflow file.'),
              'input_file_processing',
              SomaChoice((_t_(ProcessToSomaWorkflow.NO_FILE_PROCESSING), 0),
                                     (_t_(
                                         ProcessToSomaWorkflow.FILE_TRANSFER), 1),
                                     (_t_(
                                      ProcessToSomaWorkflow.SHARED_RESOURCE_PATH), 2),
                                     (_t_(ProcessToSomaWorkflow.BV_DB_SHARED_PATH), 3)),
              dict(defaultValue=0),
              'output_file_processing',
              SomaChoice((_t_(ProcessToSomaWorkflow.NO_FILE_PROCESSING), 0),
                                     (_t_(
                                         ProcessToSomaWorkflow.FILE_TRANSFER), 1),
                                     (_t_(ProcessToSomaWorkflow.SHARED_RESOURCE_PATH), 2)),
              dict(defaultValue=0)
            )
        options = Options()
        if ApplicationQt4GUI().edit(options):
            input_file_processing = ProcessToSomaWorkflow.NO_FILE_PROCESSING
            if options.input_file_processing == 1:
                input_file_processing = ProcessToSomaWorkflow.FILE_TRANSFER
            if options.input_file_processing == 2:
                input_file_processing = ProcessToSomaWorkflow.SHARED_RESOURCE_PATH
            if options.input_file_processing == 3:
                input_file_processing = ProcessToSomaWorkflow.BV_DB_SHARED_PATH
            output_file_processing = ProcessToSomaWorkflow.NO_FILE_PROCESSING
            if options.output_file_processing == 1:
                output_file_processing = ProcessToSomaWorkflow.FILE_TRANSFER
            if options.output_file_processing == 2:
                output_file_processing = ProcessToSomaWorkflow.SHARED_RESOURCE_PATH

            builtin_db = []
            if input_file_processing == ProcessToSomaWorkflow.BV_DB_SHARED_PATH:
                for db_setting in neuroConfig.dataPath:
                    if db_setting.builtin:
                        uuid = db_setting.expert_settings.uuid
                        if uuid:
                            builtin_db.append(uuid)
                        else:
                            print("warning ! db " + repr(
                                db_setting.directory) + " has no uuid.")

            ptowf = ProcessToSomaWorkflow(
                self.process,
              options.output,
              input_file_processing=input_file_processing,
              output_file_processing=output_file_processing,
              brainvisa_db=builtin_db,
              context=self)
            ptowf.doIt()

    def _iterateButton(self):
        self.readUserValues()
        self._iterationDialog = IterationDialog(self, self.process, self)
        self._iterationDialog.accepted.connect(self._iterateAccept)
        self._iterationDialog.show()

    def _iterateAccept(self):
        try:
            params = self._iterationDialog.getLists()
            processes = self.process._iterate(**params)
            iterationProcess = brainvisa.processes.IterationProcess(
                self.process.name + " iteration",
                                                                     processes,
                                                                     self.process.name)
            showProcess(iterationProcess)
        except:
            neuroException.showException()
            self._iterationDialog.show()

    def setValue(self, name, value):
        setattr(self.process, name, value)

    def readUserValues(self):
        if self.parameterizedWidget is not None:
            self.parameterizedWidget.readUserValues()
        if self._widgetStack is not None:
            for pw in self._widgetStack._children:
                ruv = getattr(pw, 'readUserValues', None)
                if ruv is None:
                    ruv = getattr(
                        getattr(pw, 'parameterizedWidget', None),  'readUserValues', None)
                if ruv is not None:
                    ruv()

    def createProcessExecutionEvent(self):
        event = super(ProcessView, self).createProcessExecutionEvent()
        mainThreadActions().call(event.setWindow, self)
        return event

    def saveAs(self):
        minf = getattr(self.process, '_savedAs', '')
        # workaround a bug in PyQt ? Param 5 doesn't work; try to use kwargs
        import sipconfig
        if sipconfig.Configuration().sip_version >= 0x040a00:
            minf = unicode(qt_backend.getSaveFileName(
                None, 'Save a process file', minf,
                'BrainVISA process (*.bvproc);;All files (*)', options=QFileDialog.DontUseNativeDialog))
        else:
            minf = unicode(
                QFileDialog.getSaveFileName(None, 'Save a process file', minf,
                                            'BrainVISA process (*.bvproc);;All files (*)', None, QFileDialog.DontUseNativeDialog))
        if minf:
            if not minf.endswith('.bvproc'):
                minf += '.bvproc'
            self.readUserValues()
            event = self.createProcessExecutionEvent()
            self.process._savedAs = minf
            event.save(minf)

    def saveProcessSetupsGUI(self):
        from brainvisa.processing.qt4gui.ProcessSetupsGUI import SaveProcessSetupsGUI
        save_process_setups_GUI = SaveProcessSetupsGUI(self)
        save_process_setups_GUI.show()
        save_process_setups_GUI.exec_()

    def clone(self):
        self.readUserValues()
        clone = brainvisa.processes.getProcessInstanceFromProcessEvent(
            self.createProcessExecutionEvent())
        return showProcess(clone)

    def show_process_doc(self):
        global _mainWindow
        doc = brainvisa.processes.getHTMLFileName(self.process)
        if os.path.exists(doc):
            _mainWindow.info.setSource(doc)

    @staticmethod
    def open():
        import sipconfig
        if sipconfig.Configuration().sip_version >= 0x040a00:
            minf = unicode(qt_backend.getOpenFileName(
                None, _t_('Open a process file'), '',
                'BrainVISA process (*.bvproc);;All files (*)',
                options=QFileDialog.DontUseNativeDialog))
        else:
            minf = unicode(QFileDialog.getOpenFileName(
                None, _t_('Open a process file'), '',
                'BrainVISA process (*.bvproc);;All files (*)', None,
                QFileDialog.DontUseNativeDialog))
        if minf:
            showProcess(brainvisa.processes.getProcessInstance(minf))


#----------------------------------------------------------------------------
def showProcess(process_id, *args, **kwargs):
    '''Opens a process window and set the corresponding arguments'''
    global _mainWindow
    view = None
    try:
        process = brainvisa.processes.getProcessInstance(process_id)
        if process is None:
            raise RuntimeError(neuroException.HTMLMessage(
                _t_('Invalid process <em>%s</em>') % (str(process_id), )))
        for i in xrange(len(args)):
            k, p = process.signature.items()[i]
            process.setValue(k, args[i])
        for k, v in kwargs.items():
            process.setValue(k, v)
        gui = getattr(process, 'overrideGUI', None)
        if gui is None:
            view = ProcessView(process)
        else:
            view = gui()
        windowGeometry = getattr(process, '_windowGeometry', None)
        if windowGeometry is not None:
            view.move(*windowGeometry['position'])
            view.resize(*windowGeometry['size'])
        view.show()
    except:  # an exception can occur if the process is reloaded and an error has been introduced in its code.
        neuroException.showException()
    return view


#----------------------------------------------------------------------------
class IterationDialog(QDialog):

    def __init__(self, parent, parameterized, context):
        QDialog.__init__(self, parent)
        self.setModal(True)
        layout = QVBoxLayout()
        self.setLayout(layout)
        layout.setContentsMargins(10, 10, 10, 10)
        self.setWindowTitle(_t_('%s iteration') %
                            unicode(parent.windowTitle()))

        params = []
        for (n, p) in parameterized.signature.items():
            if neuroConfig.userLevel >= p.userLevel:
                params += [n, neuroData.ListOf(p)]
        self.parameterized = brainvisa.processes.Parameterized(
            neuroData.Signature(*params))
        for n in self.parameterized.signature.keys():
            setattr(self.parameterized, n, None)

        self.parameterizedWidget = ParameterizedWidget(
            self.parameterized, None)
        layout.addWidget(self.parameterizedWidget)

        w = QWidget()
        hb = QHBoxLayout()
        w.setLayout(hb)
        layout.addWidget(w)
        hb.setContentsMargins(5, 5, 5, 5)
        w.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed))
        btn = QPushButton(_t_('Ok'))
        hb.addWidget(btn)
        btn.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        btn.clicked.connect(self.accept)
        btn = QPushButton(_t_('Cancel'))
        hb.addWidget(btn)
        btn.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        btn.clicked.connect(self.reject)

    def getLists(self):
        result = {}
        for n in self.parameterized.signature.keys():
            result[n] = getattr(self.parameterized, n, None)
        return result

    def accept(self):
        self.parameterizedWidget.readUserValues()
        QDialog.accept(self)

#----------------------------------------------------------------------------


class UserDialog(QDialog):

    def __init__(self, parent, modal, message, signature, buttons):
        flags = Qt.Window | Qt.Dialog
        QDialog.__init__(self, parent, flags)
        if modal:
            self.setWindowModality(Qt.WindowModal)
        self.setAttribute(Qt.WA_DeleteOnClose, True)
        layout = QVBoxLayout()
        self.setLayout(layout)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(5)

        self.condition = None
        self.signature = signature
        self._currentDirectory = None
        if message is not None:
            lab = QLabel(unicode(message))
            lab.setWordWrap(True)
            layout.addWidget(lab)
            lab.setSizePolicy(
                QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed))

        self.editors = {}
        if signature is not None:
            sv = WidgetScrollV()
            layout.addWidget(sv)
            first = None
            svWidget = QWidget()
            svWidgetLayout = QVBoxLayout()
            svWidget.setLayout(svWidgetLayout)
            for (k, p) in self.signature.items():
                hb = QHBoxLayout()
                svWidgetLayout.addLayout(hb)
                l = QLabel(k + ': ')
                hb.addWidget(l)
                e = p.editor(None, k, self)
                hb.addWidget(e)
                self.editors[k] = e
                if first is None:
                    first = e
            sv.setWidget(svWidget)

        self.group1 = QButtonGroup()
        group1Widget = QWidget()
        group1Layout = QHBoxLayout()
        group1Widget.setLayout(group1Layout)
        layout.addWidget(group1Widget)
        self._actions = {}
        self.group2 = QButtonGroup()
        group2Widget = QWidget()
        group2Layout = QHBoxLayout()
        group2Widget.setLayout(group2Layout)
        layout.addWidget(group2Widget)
        deleteGroup1 = 1
        i = 0
        for b in buttons:
            if type(b) in (tuple, list):
                caption, action = b
                btn = QPushButton(unicode(caption))
                group1Layout.addWidget(btn)
                self.group1.addButton(btn, i)
                btn.setSizePolicy(
                    QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
                self._actions[self.group1.id(btn)] = action
                deleteGroup1 = 0
            else:
                btn = QPushButton(unicode(b))
                group2Layout.addWidget(btn)
                self.group2.addButton(btn, i)
                btn.setSizePolicy(
                    QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
            i += 1
        if deleteGroup1:
            group1Widget.close()
        else:
            group1Widget.setSizePolicy(
                QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed))
            self.group1.buttonClicked[int].connect(self._doAction)
        group2Widget.setSizePolicy(
            QSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed))
        self.group2.buttonClicked[int].connect(self.select)

    def select(self, value):
        for e in self.editors.values():
            e.checkValue()
        self._result = value
        self.done(1)

    def setValue(self, name, value):
        mainThreadActions().push(self.editors[name].setValue, value)

    def getValue(self, name):
        return mainThreadActions().call(self.editors[name].getValue)

    def _doAction(self, index):
        self._actions[index](self)

    def call(self):
        if neuroConfig.gui:
            self._result = None
            mainThreadActions().call(self.show)
            mainThreadActions().call(self.exec_)
            result = self._result
            del self._result
            return result
        return -1


#----------------------------------------------------------------------------
class ProcessEdit(QDialog):

    def __init__(self, process):
        QDialog.__init__(self, None)
        layout = QVBoxLayout()
        self.setLayout(layout)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(5)
        neuroConfig.registerObject(self)

        self.process = process
        t = _t_(process.name)
        self.setWindowTitle(t)

        spl = QSplitter(Qt.Vertical)
        layout.addWidget(spl)
        w = QWidget(spl)
        vb = QVBoxLayout()
        w.setLayout(vb)

        self.labName = QLabel('<center><h3>' + t + '</h3></center>', spl)
        vb.addWidget(self.labName)

        hb = QHBoxLayout()
        vb.addLayout(hb)
        l = QLabel(_t_('HTML Path') + ': ')
        hb.addWidget(l)
        self.leHTMLPath = QLineEdit()
        hb.addWidget(self.leHTMLPath)
        hb = QHBoxLayout()
        vb.addLayout(hb)
        l = QLabel(_t_('Language') + ': ')
        hb.addWidget(l)
        self.cmbLanguage = QComboBox()
        self.cmbLanguage.setEditable(False)
        hb.addWidget(self.cmbLanguage)
        for i in neuroConfig._languages:
            self.cmbLanguage.addItem(i)
            if i == neuroConfig.language:
                self.cmbLanguage.setCurrentIndex(self.cmbLanguage.count() - 1)
        self.cmbLanguage.activated.connect(self.changeLanguage)

        l = QLabel(_t_('Short description') + ':')
        vb.addWidget(l)
        self.mleShort = QPlainTextEdit()
        vb.addWidget(self.mleShort)

        w = QWidget(spl)
        vb = QVBoxLayout()
        w.setLayout(vb)
        # spl.setLayout(vb)
        hb = QHBoxLayout()
        vb.addLayout(hb)
        l = QLabel(_t_('Parameter') + ': ')
        hb.addWidget(l)
        self.cmbParameter = QComboBox()
        self.cmbParameter.setEditable(False)
        hb.addWidget(self.cmbParameter)
        stack = QStackedWidget()
        vb.addWidget(stack)
        self.mleParameters = {}
        for n in self.process.signature.keys():
            mle = QPlainTextEdit()
            vb.addWidget(mle)
            stack.addWidget(mle)
            self.mleParameters[self.cmbParameter.count()] = mle
            self.cmbParameter.addItem(n)
            self.cmbParameter.activated.connect(stack.setCurrentIndex)
        stack.setCurrentIndex(0)

        w = QWidget(spl)
        vb = QVBoxLayout()
        w.setLayout(vb)
        l = QLabel(_t_('Long description') + ':')
        vb.addWidget(l)
        self.mleLong = QPlainTextEdit()
        vb.addWidget(self.mleLong)

        self.readDocumentation()
        self.setLanguage(unicode(self.cmbLanguage.currentText()))

        w = QWidget()
        hb = QHBoxLayout()
        w.setLayout(hb)
        vb.addWidget(w)
        hb.setContentsMargins(5, 5, 5, 5)
        w.setSizePolicy(
            QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed))
        btn = QPushButton(_t_('apply'))
        hb.addWidget(btn)
        btn.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        btn.clicked.connect(self.applyChanges)

        btn = QPushButton(_t_('Ok'))
        hb.addWidget(btn)
        btn.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        btn.clicked.connect(self.accept)

        btn = QPushButton(_t_('Cancel'))
        hb.addWidget(btn)
        btn.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        btn.clicked.connect(self.reject)

        self.resize(800, 600)

    def closeEvent(self, event):
        neuroConfig.unregisterObject(self)
        QWidget.closeEvent(self, event)

    def readDocumentation(self):
        try:
            self.documentation = brainvisa.processes.readProcdoc(self.process)
        except:
            import traceback
            traceback.print_exc()

    def writeDocumentation(self):
        brainvisa.processes.procdocToXHTML(self.documentation)
        self.setLanguage(self.language)
        brainvisa.processes.writeProcdoc(self.process, self.documentation)

    def setLanguage(self, lang):
        self.leHTMLPath.setText(
            XHTML.html(self.documentation.get('htmlPath', '')))
        self.language = lang
        d = self.documentation.get(lang, {})
        self.mleShort.setPlainText(XHTML.html(d.get('short', '')))
        self.mleLong.setPlainText(XHTML.html(d.get('long', '')))
        p = d.get('parameters', {})
        for i, j in self.mleParameters.items():
            j.setPlainText(
                XHTML.html(p.get(unicode(self.cmbParameter.itemText(i)), '')))

    def saveLanguage(self):
        d = {}
        d['short'] = self.escapeXMLEntities(
            unicode(self.mleShort.toPlainText()))
        d['long'] = self.escapeXMLEntities(
            unicode(self.mleLong.toPlainText()))
        d['parameters'] = p = {}
        for i, j in self.mleParameters.items():
            p[unicode(self.cmbParameter.itemText(i))] = self.escapeXMLEntities(
                unicode(j.toPlainText()))
        self.documentation[self.language] = d
        htmlPath = unicode(self.leHTMLPath.text())
        if htmlPath:
            self.documentation['htmlPath'] = htmlPath
        else:
            try:
                del self.documentation['htmlPath']
            except KeyError:
                pass

    @staticmethod
    def escapeXMLEntities(s):
        return re.sub(r'&(?![a-z]+;)', '&amp;', s)

    def changeLanguage(self):
        self.saveLanguage()
        self.setLanguage(unicode(self.cmbLanguage.currentText()))

    def applyChanges(self):
        self.saveLanguage()
        self.writeDocumentation()
        brainvisa.processes.generateHTMLProcessesDocumentation(self.process)
        mainWindow().info.reload()

    def accept(self):
        self.applyChanges()
        QDialog.accept(self)


#----------------------------------------------------------------------------
class ProcessSelectionWidget(QMainWindow):

    """
    This widget is the main window in brainvisa.
    Provides navigation among processes.
    """

    # Soma-Workflow widget for workflow execution on various computing resources
    # SomaWorkflowWidget
    sw_widget = None

    sw_mini_widget = None

    def __init__(self):
        QMainWindow.__init__(self)

        if getattr(ProcessSelectionWidget, '_pixmapCache', None) is None:
            ProcessSelectionWidget._pixmapCache = {}
            for file in ('icon_process_0.png', 'icon_process_1.png', 'icon_process_2.png', 'icon_process_3.png', 'folder.png'):
                fullPath = os.path.join(neuroConfig.iconPath, file)
                ProcessSelectionWidget._pixmapCache[
                    fullPath] = QIcon(fullPath)

        centralWidget = QWidget()
        self.setCentralWidget(centralWidget)

        self.dock_doc = QDockWidget("Documentation", self)
        self.dock_doc.setObjectName("documentation_dock")
        self.dock_doc.toggleViewAction().setText(_t_("Documentation"))
        self.dock_doc.setAllowedAreas(
            QtCore.Qt.LeftDockWidgetArea | QtCore.Qt.RightDockWidgetArea)
        self.dock_doc.show()
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_doc)

        self.dock_sw = QDockWidget("Execution", self)
        self.dock_sw.setObjectName("execution_dock")
        self.dock_sw.toggleViewAction().setText(_t_("Workflow execution"))
        self.dock_sw.setAllowedAreas(
            QtCore.Qt.BottomDockWidgetArea | QtCore.Qt.TopDockWidgetArea)
        if _workflow_application_model != None:

            self.sw_widget = SomaWorkflowWidget(_workflow_application_model,
                                                computing_resource=None,
                                                parent=None)
            self.sw_widget.setWindowTitle(_t_("Workflow execution"))
            self.sw_mini_widget = SomaWorkflowMiniWidget(
                _workflow_application_model,
                                                         self.sw_widget,
                                                         self.dock_sw)
            self.dock_sw.setWidget(self.sw_mini_widget)

            self.dock_sw.hide()
            self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_sw)
        else:
            self.dock_sw.hide()
            self.dock_sw.toggleViewAction().setVisible(False)
            self.sw_widget = None

        self.setCorner(
            QtCore.Qt.BottomRightCorner, QtCore.Qt.RightDockWidgetArea)
        self.setCorner(
            QtCore.Qt.BottomLeftCorner, QtCore.Qt.LeftDockWidgetArea)
        self.setCorner(QtCore.Qt.TopLeftCorner, QtCore.Qt.LeftDockWidgetArea)
        self.setCorner(QtCore.Qt.TopRightCorner, QtCore.Qt.RightDockWidgetArea)

        # Menu setup
        menu = self.menuBar()
        addBrainVISAMenu(self, menu)
        neuroConfigGUI.addSupportMenu(self, menu)
        view_menu = menu.addMenu(_t_("&View"))
        view_menu.addAction(self.dock_doc.toggleViewAction())
        view_menu.addAction(self.dock_sw.toggleViewAction())
        view_menu.addAction(close_viewers_action(self))

        # central widget layout
        layout = QVBoxLayout()
        centralWidget.setLayout(layout)
        layout.setContentsMargins(10, 10, 10, 10)

        # processTrees and the search box
        w = QWidget(self)
        layout.addWidget(w)
        vb = QVBoxLayout()
        vb.setContentsMargins(0, 0, 0, 0)
        w.setLayout(vb)
        self.currentProcessId = None
        self.processTrees = ProcessTreesWidget()
        self.processTrees.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,
                                                    QSizePolicy.Preferred))
        self.processTrees.selectionChanged.connect(self.itemSelected)
        self.processTrees.doubleClicked.connect(self.openProcess)
        self.processTrees.openProcess.connect(self.openProcess)
        self.processTrees.editProcess.connect(self.editProcess)
        self.processTrees.iterateProcess.connect(self.iterateProcess)
        # the hacked search box
        p = os.path.join(os.path.dirname(__file__), 'searchbox.ui')
        self.searchbox = QWidget()  # for PySide/PyQt compat
        self.searchbox = loadUi(p, self.searchbox)
        self.searchboxSearchB = self.searchbox.BV_search
        self.matchedProcs = []
        self.searchboxResetSearchB = self.searchbox.BV_resetsearch
        self.searchboxLineEdit = self.searchbox.BV_searchlineedit
        self._continueSearching = 0
        self.searchboxSearchB.clicked.connect(self.buttonSearch)
        self.searchboxResetSearchB.clicked.connect(self.resetSearch)

        vb.addWidget(self.processTrees)
        vb.addWidget(self.searchbox)

        # right dock : documentation panel

        self.info = HTMLBrowser(self)
        self.info.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,
                                            QSizePolicy.Expanding))
        self.dock_doc.setWidget(self.info)

        self.btnOpen = QPushButton(_t_('Open'))
        self.btnEdit = None

        self.updateList()

        # try to start with a doc opened
        self.info.home()
        ds = qApp.desktop().size()
        self.resize(min(1200, ds.width()), min(800, ds.height()))

        state_path = os.path.join(
            neuroConfig.homeBrainVISADir, "main_window_state.bin")
        if os.path.exists(state_path):
            state_file = QtCore.QFile(state_path)
            state_file.open(QtCore.QIODevice.ReadOnly)
            state = state_file.readAll()
            state_file.close()
            self.restoreState(state, 1)

    def keyPressEvent(self, keyEvent):
        if (keyEvent.matches(QKeySequence.Find) or keyEvent.matches(QKeySequence.FindNext)):
            if (self.searchboxLineEdit.text() == ""):
                self.info.browser.keyPressEvent(keyEvent)
            else:
                self.buttonSearch()
        elif ((keyEvent.key() == Qt.Key_W) and (keyEvent.modifiers() == Qt.ControlModifier)):
            self.info.openWeb()
        else:
            QWidget.keyPressEvent(self, keyEvent)

    def buttonSearch(self):
        """
        Called when user click on search / next button.
        The text written in the search box is searched in tree leaves names (processes).
        The first item found which name contains the searched string becomes selected. If the user click another time on the search / next button, next item is searched...
        """
        # new search
        if not self.matchedProcs:
                # searched string
            s = unicode(self.searchboxLineEdit.text()).lower()
            if s == "":
                self.matchedProcs = None
            else:
                    # search for items which name contains the string ->
                    # generator
                self.matchedProcs = self.processTrees.findItem(s)
        # next search
        if self.matchedProcs:
            try:  # an exception will occur when there is no more items
                item = next(self.matchedProcs)
                self.searchboxLineEdit.setEnabled(False)
                self.searchboxSearchB.setText('next')
#        self.searchboxSearchB.setShortcut( QKeySequence.FindNext )
            except:
                self.resetSearch()

    def resetSearch(self):
        """
        Called at the end of a search or when the user click on reset button.
        """
        self.matchedProcs = None
        self.searchboxSearchB.setText('search')
#    self.searchboxSearchB.setShortcut( QKeySequence.Find )
        self.searchboxLineEdit.setEnabled(True)
        self.searchboxLineEdit.setText("")

    def itemSelected(self, item):
        """
        Called when a tree item becomes selected. currentProcessId is updated and associated documentation is shown.

        :param item: the newly selected item :py:class:`ProcessTree.Item`
        """
        if item:
            if item.isLeaf():
                processId = item.id
                self.currentProcessId = processId
                self.btnOpen.setEnabled(1)
                try:
                    documentation = brainvisa.processes.readProcdoc(
                        self.currentProcessId)
                    source = brainvisa.processes.getHTMLFileName(
                        self.currentProcessId)
                    if os.path.exists(source):
                        self.info.setSource(source)
                    else:
                        self.info.setText('')
                except:
                    import traceback
                    traceback.print_exc()
                if self.btnEdit is not None:
                    self.btnEdit.setEnabled(1)
            else:
                self.currentProcessId = None
                self.btnOpen.setEnabled(0)
                if self.btnEdit is not None:
                    self.btnEdit.setEnabled(0)
                # Construct categroy HTML documentation file name
                self.info.showCategoryDocumentation(item.id)
        else:
            self.info.setText('')

    def openProcess(self, item=None):
        """
        Called to open current process.
        If the process is not given, selected process in current tree is opened.

        :param item: the process to open. :py:class:`ProcessTree.Item`
        """
        processId = None
        if item is not None:  # open given item
            if item.isLeaf():
                processId = item.id
                showProcess(processId)
        else:  # if it is not given (open button), open selected item in current process tree
            item = self.processTrees.treeStack.currentWidget().currentItem()
            if item is not None:
                item = item.model
                if item.isLeaf():
                    processId = item.id
                    showProcess(processId)
        if processId != self.currentProcessId:
            self.itemSelected(item)

    def editProcess(self, item=None):
        processId = None
        if item is not None:
            if item.isLeaf():
                processId = item.id
        else:
            processId = self.currentProcessId
        win = ProcessEdit(brainvisa.processes.getProcessInstance(processId))
        win.show()

    def iterateProcess(self, item=None):
        processId = None
        if item is not None:
            if item.isLeaf():
                processId = item.id
        else:
            processId = self.currentProcessId
        self.currentProcess = brainvisa.processes.getProcessInstance(processId)
        # print("iterate process", processId)
        self._iterationDialog = IterationDialog(
            self, self.currentProcess, self)
        self._iterationDialog.accepted.connect(self._iterateAccept)
        self._iterationDialog.show()

    def _iterateAccept(self):
        """
        Call back when accepting iteration dialog. Iterates the selected process.
        """
        try:
            params = self._iterationDialog.getLists()
            processes = self.currentProcess._iterate(**params)
            iterationProcess = brainvisa.processes.IterationProcess(
                self.currentProcess.name + " iteration",
                                                                     processes,
                                                                     self.currentProcess.name)
            # iterationProcess.possibleChildrenProcesses =
            showProcess(iterationProcess)
        except:
            neuroException.showException()
            self._iterationDialog.show()

    def updateList(self):
        """
        Reloads the list of process trees.
        """
        self.processTrees.setModel(
            brainvisa.processes.updatedMainProcessTree())

    def closeEvent(self, event):
        state = self.saveState(1)
        state_file = QtCore.QFile(
            os.path.join(neuroConfig.homeBrainVISADir, "main_window_state.bin"))
        state_file.open(QtCore.QIODevice.WriteOnly)
        state_file.write(state)
        state_file.close()
        quitRequest()
        event.ignore()


#----------------------------------------------------------------------------
class ProcessTreesWidget(QSplitter):

    """
    A widget that shows a list of :py:class:`ProcessTree`.
    Each process tree presents a sub group of existing processes.

    It's composed of two parts :
    * the list of process trees (use profiles)
    * a view of currently selected tree
    Each process tree can be opened in another window in order to enable drag and drop from one tree to another.

    .. py:attribute:: treeIndex

      Widget containing items representing each process tree. TreeListWidget.

    .. py:attribute:: treeStack

      A stack of EditableTreeWidget, representing the content of each processTree. QWidgetStack.

    .. py:attribute::  treeStackIdentifiers

      dict associating a processTree to an unique integer identifier used with the widget stack. Only the selected processTree widget of the stack is visible.

    .. py:attribute::  widgets

      list of EditableTreeWidget currently in the stack. Useful because QWidgetStack doesn't provide iterator on its content.

    .. py:attribute:: openedTreeWidget

      Currently opened process tree. It is in a window independant from the main window. EditableTreeWidget

    .. py:attribute:: model

      list of ProcessTree which this widget represents. ProcessTrees

    .. py:attribute:: popupMenu

      QPopupMenu contextual menu associated to the list of process trees.

    .. py:attribute:: savesTimer

      QTimer started when the model has changed. When the timer times out, the model is saved. Used to delay model saves : it speeds up execution when there is several modification at the same time (drag&drop several elements).
    """

    selectionChanged = QtCore.Signal(soma.notification.ObservableAttributes)
    doubleClicked = QtCore.Signal(soma.notification.ObservableAttributes)
    openProcess = QtCore.Signal(soma.notification.ObservableAttributes)
    editProcess = QtCore.Signal(soma.notification.ObservableAttributes)
    iterateProcess = QtCore.Signal(soma.notification.ObservableAttributes)

    def __init__(self, processTrees=None, parent=None):
        """
        :param processTrees: ProcessTrees, the list of process trees which this widget represents
        :param parent: QWidget, container of this widget
        """
        QSplitter.__init__(self, parent)
        self.treeIndex = TreeListWidget(None, self, iconSize=bigIconSize)
        self.treeIndex.setSizePolicy(
            QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred))
        # signals
        self.treeIndex.currentItemChanged.connect(self.setCurrentTree)
        # on clicking on a tree, emit a pysignal for transmitting the signal to
        # the parent. The shown documentation may need to be changed. (clicked
        # instead of selectionChanged because the documentation may need to be
        # changed event if the item was already selected)
        self.treeIndex.itemClicked.connect(self.selectionChanged_slot)
        self.treeIndex.customContextMenuRequested.connect(self.openContextMenu)
        # help tooltip
        self.treeIndex.setToolTip(
            _t_("Create your own lists of processes choosing new in contextual menu.<br>To add items in a list, open an existing list and move items in the new list.<br>If you set a list as default, it will selected the next time you run brainvisa."))

        self.treeStack = QStackedWidget(self)
        self.treeStackIdentifiers = {}
        self.treeStack.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,
                                                 QSizePolicy.Preferred))
        # self.setResizeMode( self.treeIndex, QSplitter.FollowSizeHint )
        self.widgets = []
        self.openedTreeWidget = None

        # Popup Menu for toolboxes
        self.popupMenu = QMenu(self)
        _addAction(self.popupMenu, _t_("New"),  self.menuNewTabEvent)
        _addAction(self.popupMenu, _t_("Delete"),  self.menuDelTabEvent)
        _addAction(self.popupMenu, _t_("Open"), self.menuOpenTabEvent)
        _addAction(self.popupMenu, _t_("Set as default list"),
                   self.menuSetDefaultEvent)

        # Popup Menu for processes
        self.processMenu = QMenu(self)
        _addAction(self.processMenu, _t_("Open"),  self.menuOpenProcessEvent)
        _addAction(self.processMenu, _t_(
            "Edit documentation"),  self.menuEditProcessEvent)
        _addAction(self.processMenu, _t_(
            "Iterate"), self.menuIterateProcessEvent)

        self.setStretchFactor(0, 2)
        self.setStretchFactor(1, 3)
        if processTrees:
            self.setModel(processTrees)

    def setModel(self, processTrees):
        """
        The widget is initialized with the given list of process tree.
        For each process tree, an item is added in treeIndex. A widget is created to represent each process tree and added to treeStack.

        :param processTrees: the list of process trees which this widget represents
        """
        # clear widgets
        self.treeIndex.clear()
        for w in self.widgets:
            self.treeStack.removeWidget(w)
        self.treeStackIdentifiers = {}
        self.widgets = []
        # register model and add listener
        self.model = processTrees
        self.model.addListener(self.updateContent)
        # listens the change of selectedTree attribute in model
        self.model.onAttributeChange("selectedTree", self.updateSelectedTree)
        self.model.onAttributeChange("selectedTree", self.modelChanged)
        self.model.addListener(self.modelChanged)
        # for each processTree, create an EditableTreeWidget which is added to the widget stack
        # and add an element to the list index of trees
        for processTree in processTrees.values():
            self.addProcessTree(processTree)
        self.treeIndex.setModel(processTrees)

        # if there's a selected tree by default, the corresponding item in
        # widget is selected
        if self.model.selectedTree != None:
            found = False
            i = 0
            while i < self.treeIndex.topLevelItemCount() and not found:  # search selected tree corresponding item in treeIndex widget
                item = self.treeIndex.topLevelItem(i)
                i += 1
                if item.model == self.model.selectedTree:
                    found = True
                    self.treeIndex.setCurrentItem(item)

        # Create a timer to delay model saves in minf file : it speeds up
        # execution when there is several modification at the same time
        # (drag&drop several elements)
        self.savesTimer = QTimer()
        self.savesTimer.setSingleShot(True)
        self.savesTimer.timeout.connect(self.model.save)

    def addProcessTree(self, processTree):
        """
        Add elements in the widget to add a representation of this process tree.

        :param processTree: new process tree for which the widget must be completed.
        """
        treeWidget = EditableTreeWidget(processTree, self.treeStack)
        if processTree.modifiable:
            treeWidget.setToolTip(
                _t_("This list is customizable. You can :<br>- move items by drag and drop,<br>- delete item with del key,<br>- copy items by drag and drop and ctrl key,<br>- create new category with contextual menu."))
        # signals
        # selectionChanged doesn't work with multiple selection
        # currentChanged isn't emited when click on an item that has already keyboeard focus and is not emited when click on an already selected item altought it may be necessary to update documentation because several items can be selected at the same time
        # -> so use clicked signal instead
        treeWidget.itemClicked.connect(self.selectionChanged_slot)
        treeWidget.itemDoubleClicked.connect(self.doubleClicked_slot)
        treeWidget.customContextMenuRequested.connect(
            lambda p: self.openProcessMenu(treeWidget, p))
        # the new widget representing the process tree is added in treeStack
        # and in widgets
        stackIdentifier = self.treeStack.addWidget(treeWidget)
        self.treeStackIdentifiers.setdefault(
            object.__hash__(processTree), stackIdentifier)
        self.widgets.append(treeWidget)
        # listens changes in the process tree (for saving each change in minf
        # file)
        processTree.addListenerRec(self.modelChanged)
        processTree.onAttributeChangeRec("name", self.modelChanged)

    def showTreeIndex(self):
        if self.treeIndex.isHidden():
            self.treeIndex.show()
        else:
            self.treeIndex.hide()

    def openContextMenu(self, point):
        """
        Called on contextMenuRequested signal. It opens the popup menu at cursor position.
        """
        self.popupMenu.exec_(QCursor.pos())

    def openProcessMenu(self, listView, point):
        """
        Called on contextMenuRequested signal on the list of processes of a toolbox. It opens the process menu at cursor position if the current item represents a process.
        """
        item = listView.itemAt(point)
        if item and item.model and item.model.isLeaf():
            self.processMenu.exec_(QCursor.pos())
        else:
            listView.openContextMenu(point)

    def updateContent(self, action=None, items=None, position=None):
        """
        Called on model change (list of process trees). The widget must update itself to reflect the change.
        """
        # treeIndex is a TreeListWidget and has already a listener which update the view on model changes
        # but some changes imply modification of treeStack -> add a widget in
        # the stack or remove a widget
        if action == ObservableList.INSERT_ACTION:  # add a new process tree in the list
            for processTree in items:
                self.addProcessTree(processTree)
        elif action == ObservableList.REMOVE_ACTION:  # remove a process tree
            for processTree in items:
                w = self.treeStack.widget(
                    self.treeStackIdentifiers.get(object.__hash__(processTree)))
                self.treeStack.removeWidget(w)
                self.widgets.remove(w)

    def updateSelectedTree(self, newSelection):
        """
        Called when the selected tree changes.
        """
        pass  # maybe set a graphical element to show it is the default list...

    def modelChanged(self, action=None, items=None, position=None):
        """
        Method registred to be called when a process tree has changed or when the list of tree has changed.
        New ProcessTree list must be saved in a minf file.
        If change is insertion of a new item in a tree, registers listeners on this new item.
        """
        # print("model changed", action, "write to minf file
        # processTrees.minf")
        if action == ObservableList.INSERT_ACTION:
            for item in items:
                # on insertion in a tree, listen changes of the new element
                # on insertion of a tree in the tree list, nothing to do,
                # listeners are already registred
                if not issubclass(item.__class__, brainvisa.processes.ProcessTree):
                    if not item.isLeaf():
                        item.addListenerRec(self.modelChanged)
                        item.onAttributeChangeRec("name", self.modelChanged)
                    else:
                        item.onAttributeChange("name", self.modelChanged)
        # instead of systematic model save for each change, start a timer wich will timeout when current event is finished
        # So if there is several modification due to the same event (drag and
        # drop several elements), the model will be saved only one time when
        # all changes are done. (speedier)
        if not self.savesTimer.isActive():  # if the timer is already started nothing to do, the change will be save anyway
            self.savesTimer.start(0)
        # self.model.save()

    def selectionChanged_slot(self, item, col=0):
        """
        Called when selected item has changed in current process tree.
        This method emits a signal that must be caught by parent widget.
        """
        if item is not None:
            self.selectionChanged.emit(item.model)

    def doubleClicked_slot(self, item, col):
        """
        Called on double click on an item of current process tree.
        This method emits a signal that must be caught by parent widget.
        """
        self.doubleClicked.emit(item.model)

    def setCurrentTree(self, item, previous=None):
        """
        Changes the visible widget in the stack.
        """
        if item:
            self.treeStack.setCurrentIndex(
                self.treeStackIdentifiers.get(object.__hash__(item.model)))

    def findItem(self, name):
        """
        Find items that contain the string given in parameters in their name. Each found item is selected and yield (and replace previous selection).
        Wide search.

        :param name: string searched in items names.

        :rtype:  generator
        """
        for widget in self.widgets:  # for all process trees widgets
            it = QTreeWidgetItemIterator(widget)
            lastSelection = None
            while it.value():
                item = it.value()
                if not item.isHidden():
                    if item.model.isLeaf():  # for a leaf (process) search string in name
                        keep = False
                        if item.model.name.lower().find(name) > -1 \
                                or item.model.id.lower().find(name) > -1:
                            keep = True
                        else:
                            # also try to find the untranslated process name
                            try:
                                proc = brainvisa.processes.getProcess(
                                    item.model.id)
                                pname = proc.name
                                if pname.lower().find(name) > -1:
                                    keep = True
                            except:
                                pass
                        if keep:
                            self.select(widget, item, lastSelection)
                            lastSelection = (widget, item)
                            yield item
                it += 1

    def select(self, widget, item, lastSelection):
        """
        Select a process tree and an item in it. Undo last selection.

        :param widget:  EditableTreeWidget, the tree widget that contains the item to select.
        :param item: EditableTreeItem, the item (process) to select
        :param lastSelection: tuple(EditableTreeWidget, EditableTreeItem), previous selected item and its container, to be unselected.
        """
        self.selectIndex(widget.model)  # select in left panel (toolbox name)
        self.setCurrentTree(widget)  # raise widget of toolbox content
        item.setHidden(False)
        # widget.ensureItemVisible( item ) # show item (open parents...)
        widget.setCurrentItem(item)  # select item
        if lastSelection:  # undo last selection
            lastSelection[1].setSelected(False)
            # lastSelection[0].setSelected(lastSelection[1], 0)

    def selectIndex(self, model):
        """
        Select a process tree in the left panel (toolboxes).

        :param model: the process tree to select
        """
        i = 0
        found = False
        while i < self.treeIndex.topLevelItemCount() and not found:  # search selected tree corresponding item in treeIndex widget
            item = self.treeIndex.topLevelItem(i)
            if item.model == model:
                found = True
                self.treeIndex.setCurrentItem(item)
            i += 1

    #------ context menu events ------
    def menuNewTabEvent(self):
        """
        Called on click on new option in contextual menu.
        Adds a new empty tree in the model.
        """
        processTree = brainvisa.processes.ProcessTree(
            name='Personal Bookmarks')
        processTree.new = True
        self.model.add(processTree)

    def menuDelTabEvent(self):
        """
        Called on click on del option in contextual menu.
        Removes the selected tree from the model.
        """
        item = self.treeIndex.currentItem()
        if item:
            if item.model.modifiable:
                del self.model[item.model.id]

    def menuOpenTabEvent(self):
        """
        Called on click on open option in contextual menu.
        Opens selected tree in a new window.
        """
        item = self.treeIndex.currentItem()
        if item:
            self.openedTreeWidget = EditableTreeWidget(item.model)
            self.openedTreeWidget.resize(400, 600)
            self.openedTreeWidget.show()

    def menuSetDefaultEvent(self):
        """
        Called on click on set default option in contextual menu.
        Sets the selected tree as the default selected tree. So on next run of brainvisa, this tree will be selected.
        """
        item = self.treeIndex.currentItem()
        if item:
            self.model.selectedTree = item.model

    def menuOpenProcessEvent(self):
        """
        Called on click on open option in process menu.
        Emits a signal for the parent window which will open the process.
        """
        item = self.treeStack.currentWidget().currentItem()
        if item:
            print('item.model:', type(item.model))
            self.openProcess.emit(item.model)

    def menuEditProcessEvent(self):
        """
        Called on click on edit option in process menu.
        Emits a signal for the parent window which will edit the process.
        """
        item = self.treeStack.currentWidget().currentItem()
        if item:
            self.editProcess.emit(item.model)

    def menuIterateProcessEvent(self):
        """
        Called on click on iterate option in process menu.
        Emits a signal for the parent window which will iterate the process.
        """
        item = self.treeStack.currentWidget().currentItem()
        if item:
            self.iterateProcess.emit(item.model)

#----------------------------------------------------------------------------


class MainWindow(QWidget):

    def __init__(self):
        QWidget.__init__(self)
        self.myLayout = QVBoxLayout(self)
        self.setLayout(self.myLayout)
        self.mainModules = []
        self.subLayouts = []
        neuroConfig.registerObject(self)

    def closeEvent(self, event):
        neuroConfig.unregisterObject(self)
        QWidget.closeEvent(self, event)

    def addMainModule(self, identifier, name, image, description):
        # Create main module widget
        w = QWidget(self)
        layout = QVBoxLayout(w)
        w.setLayout(layout)
        l = QLabel(_t_(name), self)
        layout.addWidget(l)
        hb = QHBoxLayout()
        layout.addLayout(hb)
        l = QLabel()
        hb.addWidget(l)
        l.setIcon(
            QIcon(
                os.path.join(
                    neuroConfig.mainPath, 'doc', 'images', image))),
        l.setSizePolicy(QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed))
        l.resize(320, 200)
        l = QTextEdit()
        hb.addWidget(l)
        l.setText(description)
        l.setReadOnly(True)

        if (len(self.mainModules) % 2 == 0):
            self.subLayouts.append(QHBoxLayout(self.layout()))
        self.subLayouts[-1].addWidget(w)
        w.show()
        self.mainModules.append(w)


#----------------------------------------------------------------------------
class RemoteContextGUI(QTreeWidgetItem):

    """
    Specific GUI to display messages returned by processes executed remotely.
    """

    def __init__(self, parent, name='Remote Hosts:'):
        """
        The specific GUI is a QListView. It is composed of an arborescence of QListViewItems that sorts
        the messages according to the process and the host they belong to::

        Remote Hosts:
        |
        --host
          |
          --process
            |
            --message
            --message
          --process
            |
            --message

        :param parent: the QListView.
        :param name: name
        """

        remoteView = QTreeWidget(parent)
        remoteView.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,
                                             QSizePolicy.Preferred))

        remoteView.setWindowTitle('Remote messages')
        remoteView.setColumnCount(4)
        remoteView.setHeaderLabels(['IP', 'ID', 'Status', 'Messages'])

        QTreeWidgetItem.__init__(self, remoteView)
        self.setText(0, name)

        self.setExpanded(True)

        self.processList = {}
        self.ipList = {}

    def addIP(self, ip):
        i_item = QTreeWidgetItem(self, [ip])
        self.ipList[str(ip)] = i_item
        i_item.setExpanded(True)

    def addProcess(self, ip, pid, status=' Starting...', message=''):
        p_item = QTreeWidgetItem(self.ipList[str(ip)],
                                 ['Process', '%03d' % pid, status, message])
        # p_item.setText( 0, 'Process' )
        # p_item.setText( 1, '%03d'%pid )
        # p_item.setText( 2, status )
        # p_item.setText( 3, message )
        self.processList[str(pid)] = p_item
        # self.ipList[str(ip)].insertItem(p_item)

    def addMessage(self, pid, message):
        m_item = QTreeWidgetItem(self.processList[str(pid)],
                                 ['Message', '', '', message])
        # m_item.setText( 0, 'Message' )
        # m_item.setText( 1, '' )
        # m_item.setText( 2, '' )
        # m_item.setText( 3, message )
        # self.processList[str(pid)].insertItem(m_item)

    def setProcessStatus(self, pid, status):
        self.processList[str(pid)].setText(2, status)

    def setCurrentMessage(self, pid, message):
        self.processList[str(pid)].setText(3, message)

    def clear(self):
        for item in self.ipList.values():
            self.takeChild(self.indexOfChild(item))
            del(item)

        self.processList = {}
        self.ipList = {}

    def sort(self):
        pass

    def sortChildItems(self, col, asc):
        pass

#----------------------------------------------------------------------------


def mainWindow():
    global _mainWindow
    return _mainWindow


def showMainWindow():
    global _mainWindow
    if neuroConfig.openMainWindow:
        #_mainWindow = ProcessSelection()
        # window with customizable lists of processes
        _mainWindow = ProcessSelectionWidget()
        _mainWindow.show()
        for w in qApp.topLevelWidgets():
            if w is not _mainWindow:

                w.raise_()
    else:
        _mainWindow = None


def close_viewers_action(parent):
    action = QAction(parent)
    action.setText(_t_("Close all viewers"))
    action.triggered.connect(close_viewers)
    return action


def close_viewers():
    from brainvisa.data.qt4gui.readdiskitemGUI import DiskItemEditor
    from brainvisa.data.qt4gui.hierarchyBrowser import HierarchyBrowser
    for w in qApp.allWidgets():
        if isinstance(w, DiskItemEditor):
            w.close_viewer()
        elif isinstance(w, ProcessView):
            process_info = brainvisa.processes.getProcessInfo(w.process.id())
            if process_info is not None and "viewer" in process_info.roles:
                w.close()
        elif isinstance(w, HierarchyBrowser):
            w.close_viewers()

#----------------------------------------------------------------------------


def updateProcessList():
    _mainWindow.updateList()


def reloadToolboxesGUI():
    """
    Calls :py:func:`brainvisa.processes.reloadToolboxes` and updates the main window (list of toolboxes or processes may have changed).
    If some databases should be updated, the user is warned.
    """
    QtGui.QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
    # close all processes windows because processes will be reinstantiated
    saved = save_and_close_all_processes()
    brainvisa.processes.reloadToolboxes()
    updateProcessList()
    from brainvisa.data.qt4gui.updateDatabases import warnUserAboutDatabasesToUpdate
    warnUserAboutDatabasesToUpdate()
    # reopen processes
    restore_all_processes(saved)
    QtGui.QApplication.restoreOverrideCursor()


def loadProcessSetupsGUI():
    from brainvisa.processing.qt4gui.ProcessSetupsGUI import LoadProcessSetupsGUI
    load_process_setups_GUI = LoadProcessSetupsGUI()
    load_process_setups_GUI.show()
    load_process_setups_GUI.exec_()


def save_and_close_all_processes():
    close_viewers()
    saved = []
    for w in qApp.allWidgets():
        if isinstance(w, ProcessView):
            # dont' remember updateDatabases process, it will be shown
            # again if needed after reload.
            if w.process.id() != 'updateDatabases':
                w.readUserValues()
                clone = w.createProcessExecutionEvent()
                saved.append(clone)
            w.close()
    return saved


def restore_all_processes(saved):
    uis = []
    for event in saved:
        proc = brainvisa.processes.getProcessInstanceFromProcessEvent(event)
        procui = showProcess(proc)
        uis.append(procui)
    return uis

#----------------------------------------------------------------------------


def initializeProcessesGUI():

    global _computing_resource_pool, _workflow_application_model

    import brainvisa.processes
    brainvisa.processes.setMainThreadActionsMethod(QtThreadCall())
    _computing_resource_pool = None
    _workflow_application_model = None

    if neuroConfig.gui:
        if _soma_workflow and neuroConfig.userLevel >= 1:
            _computing_resource_pool = ComputingResourcePool()
            _computing_resource_pool.add_default_connection()
            _workflow_application_model = WorkflowApplicationModel(
                _computing_resource_pool)

        six.exec_(
            'from brainvisa.processing.qt4gui.neuroProcessesGUI import *',
                  brainvisa.processes.__dict__)
        brainvisa.processes._defaultContext = ExecutionContextGUI()


def html_with_local_images(html):
    images_basepath = neuroConfig.getDocPath('images', project='axon') \
        + '-' + neuroConfig.shortVersion
    html_repl = html.replace(' src="../../../../',
                             ' src="%s/' % images_basepath)
    html_repl = html_repl.replace(' SRC="../../../../',
                                  ' src="%s/' % images_basepath)
    html_repl = html_repl.replace(' src="../../',
                                  ' src="%s/' % images_basepath)
    html_repl = html_repl.replace(' SRC="../../',
                                  ' src="%s/' % images_basepath)
    return html_repl
