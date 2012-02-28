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

import os
from PyQt4 import uic, QtGui, QtCore
from PyQt4.QtCore import Qt
from soma.minf.api import readMinf
from neuroProcesses import getProcessInstanceFromProcessEvent
from neuroProcessesGUI import ProcessView
from qt4gui.neuroLogGUI import LogItemsViewer

class DataHistoryWindow(QtGui.QMainWindow):
  """
  :Attributes:
  
  .. py:attribute:: data
  
    The :py:class:`DiskItem` whose history is displayed in this window.
  """
  data = None
  ui = None
  
  
  def __init__(self, data, bvproc_uuid, parent=None):
    super(DataHistoryWindow, self).__init__(parent)
    
    UiDataHistory = uic.loadUiType(os.path.join(os.path.dirname( __file__ ), 
                                                'dataHistory.ui' ))[0]
    self.ui = UiDataHistory()
    self.ui.setupUi(self)
    self.setAttribute(Qt.WA_DeleteOnClose)

    self.data = data
    self.bvproc_uuid = bvproc_uuid

    # menu bar
    self.process_menu = self.ui.menubar.addMenu("&Process")

    self.ui.info.setText("History of "+self.data.fullPath())
    
    if self.bvproc_uuid is not None:
      bvproc_file = os.path.join(self.data.get("_database", ""), "history_book", self.bvproc_uuid + ".bvproc")
      if os.path.exists(bvproc_file):
        minf = readMinf(bvproc_file)
        if minf:
          process_event=minf[0]
          process=getProcessInstanceFromProcessEvent(process_event)
          if process is not None:
            self.show_process(process)
          bvsession=process_event.content.get("bvsession", None)
          log=process_event.content.get("log", None)
          if log:
            self.show_log(log, bvsession)

  #def closeEvent( self, event ):
    #if self.process_view:
      #self.process_view.cleanup()
    #QtGui.QMainWindow.closeEvent( self, event )


  def show_process(self, process):
    if process:
      QtGui.QApplication.setOverrideCursor(QtGui.QCursor(Qt.WaitCursor))
      try:
        process_view = ProcessView(process, parent=self, read_only=True)
      finally:
        QtGui.QApplication.restoreOverrideCursor()
      process_view.inlineGUI.hide()
      process_view.info.hide()
      self.ui.process_widget.layout().addWidget(process_view)
      process_view.action_clone_process.setText("Edit")
      self.process_menu.addAction(process_view.action_clone_process)
  
  
  def show_log(self, log, bvsession):
    session_item=None
    bvsession_file = os.path.join(self.data.get("_database", ""), "history_book", str(bvsession) + ".bvsession")
    if os.path.exists(bvsession_file):
      minf = readMinf(bvsession_file)
      if minf:
        session_item=minf[0].content.get("log", None)

    logitems=[]
    if session_item:
      logitems.extend(session_item)
    if log:
      logitems.extend(log)
    
    logitems_viewer=LogItemsViewer(logitems, self)
    self.ui.log_widget.layout().addWidget(logitems_viewer)
    