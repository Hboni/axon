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

from brainvisa.configuration import neuroConfig

if neuroConfig.anatomistImplementation != 'socket':

  import anatomist.cpp as anacpp
  from PyQt4 import QtGui
  from soma.wip.application.api import findIconFile

  class ReusableWindowAction( QtGui.QAction ):
    def toggleReuseWindow( self, state ):
      win = self.parent()
      from brainvisa import anatomist
      a = anatomist.Anatomist( [ '-b' ] )
      a.setReusableWindow( a.AWindow( a, win, refType='WeakShared' ), state )

  class ReusableWindowHook( anacpp.EventHandler ):
    def doit( self, event ):
      if event.eventType() == 'CreateWindow':
        win = event.contents()[ '_window' ]
        win = anacpp.AWindow.fromObject( win )
        if isinstance( win, QtGui.QMainWindow ):
          toolbar = win.findChild( QtGui.QToolBar, 'mutations' )
          if toolbar is None:
            toolbar = win.findChild( QtGui.QToolBar )
            if toolbar is None:
              toolbar = win.addToolBar( 'BV toolbar' )
              if win.toolBarsVisible():
                toolbar.show()
              else:
                toolbar.hide()
          if toolbar is not None:
            toolbar.addSeparator()
            icon = QtGui.QIcon( findIconFile( 'eye.png' ))
            ac = ReusableWindowAction( icon, 'Keep and reuse in BrainVisa',
              win )
            ac.setCheckable( True )
            ac.toggled.connect( ac.toggleReuseWindow )
            toolbar.addAction( ac )


  handler = None

  def installWindowHandler():
    global handler
    handler = ReusableWindowHook()
    anacpp.EventHandler.registerHandler( 'CreateWindow', handler )
    anacpp.CommandContext.defaultContext().evfilter.filter( 'CreateWindow' )

  def uninstallWindowHandler():
    print 'clearing windows handler'
    global handler
    if handler is not None:
      anacpp.EventHandler.unregisterHandler( 'CreateWindow', handler )
      handler = None

