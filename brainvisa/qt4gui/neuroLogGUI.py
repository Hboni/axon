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

import time
import os
from backwardCompatibleQt import QWidget, QVBoxLayout, QIcon, QSplitter, Qt, QSizePolicy, QSize, QTreeWidget, QTreeWidgetItem, QTreeWidgetItemIterator, QHBoxLayout, QPushButton, QObject, SIGNAL, QFileDialog
import neuroLog
import neuroException
import neuroConfig
from soma.qtgui.api import TextEditWithSearch

class LogViewer( QWidget ):

  def __init__( self, fileName, parent=None, name=None ):
    QWidget.__init__( self, parent )
    if name:
      self.setObjectName( name )
    layout=QVBoxLayout()
    self.setLayout(layout)
    if getattr( LogViewer, 'pixIcon', None ) is None:
      setattr( LogViewer, 'pixIcon', QIcon( os.path.join( neuroConfig.iconPath, 'icon_log.png' ) ) )
    self.setWindowIcon( self.pixIcon )
    self._pixmaps = {}

    splitter = QSplitter( Qt.Horizontal )
    splitter.setSizePolicy( QSizePolicy( QSizePolicy.Minimum, QSizePolicy.Expanding ) )
    layout.addWidget(splitter)
    
    self._list = QTreeWidget( splitter )
    self._list.setColumnCount(2)
    self._list.setHeaderLabels( [ _t_('Description'), _t_('Date') ] )
    self._list.setAllColumnsShowFocus( 1 )
    self._list.setIconSize(QSize(32, 32))
    self._list.setSizePolicy( QSizePolicy( QSizePolicy.Preferred, QSizePolicy.Expanding ) )
    self._list.setRootIsDecorated( 1 )
    #splitter.setResizeMode( self._list, QSplitter.KeepSize )

    self._content = TextEditWithSearch(splitter)#QTextView( splitter )
    self._content.setReadOnly(True)
    self._content.setSizePolicy( QSizePolicy( QSizePolicy.Minimum, QSizePolicy.Expanding ) )
    #splitter.setResizeMode( self._content, QSplitter.Stretch )
    #self._content.setReadOnly( 1 )
    QObject.connect( self._list, SIGNAL( 'currentItemChanged( QTreeWidgetItem *, QTreeWidgetItem * )' ),
                     self._updateContent )

    hb = QHBoxLayout( )
    layout.addLayout(hb)
    hb.setMargin( 5 )
    btn = QPushButton( _t_( '&Refresh' ) )
    btn.setSizePolicy( QSizePolicy( QSizePolicy.Fixed, QSizePolicy.Fixed ) )
    hb.addWidget(btn)
    QObject.connect( btn, SIGNAL( 'clicked()' ), self.refresh )
    btn = QPushButton( _t_( '&Close' ) )
    hb.addWidget(btn)
    btn.setSizePolicy( QSizePolicy( QSizePolicy.Fixed, QSizePolicy.Fixed ) )
    QObject.connect( btn, SIGNAL( 'clicked()' ), self.close )
    btn = QPushButton( _t_( '&Open...' ) )
    hb.addWidget(btn)
    btn.setSizePolicy( QSizePolicy( QSizePolicy.Fixed, QSizePolicy.Fixed ) )
    QObject.connect( btn, SIGNAL( 'clicked()' ), self.open )

    neuroConfig.registerObject( self )
    self.setLogFile( fileName )
    self.resize( 800, 600 )
    self._list.resizeColumnToContents(0)
    firstItem = self._list.topLevelItem(0)
    if firstItem:
      self._updateContent( firstItem )

  def setLogFile( self, fileName ):
    self._fileName = fileName
    self.setWindowTitle( self._fileName )
    self.refresh()

  def closeEvent( self, event ):
    self.emit( SIGNAL( 'close' ) )
    neuroConfig.unregisterObject( self )
    QWidget.closeEvent( self, event )

  def _addLogItem( self, item, parent, after, itemIndex, listState, currentItemIndex, currentItemList ):
    viewItem = QTreeWidgetItem( parent )
    viewItem.setText( 0, item.what() )
    viewItem.setText( 1, time.asctime( time.localtime( item.when() ) ) )
    if item.icon():
      pixmap = self._pixmaps.get( item.icon() )
      if pixmap is None:
        pixmap = QIcon( os.path.join( neuroConfig.iconPath, item.icon() ) )
        self._pixmaps[ item.icon() ] = pixmap
      viewItem.setIcon( 0, pixmap )
    content = item.html()
    if content:
      self._contents[ viewItem ] = content

    itemIndex += 1
    isOpen = 0
    if itemIndex < len( listState ):
      isOpen = listState[ itemIndex ]
      if itemIndex == currentItemIndex:
        currentItemList.append( viewItem )
 
    for child in item.children():
      ( after, itemIndex ) = self._addLogItem( child, viewItem, after, itemIndex, listState, currentItemIndex, currentItemList)
    viewItem.setExpanded( isOpen )
    return ( viewItem, itemIndex )

  def _updateContent( self, item ):
    self._content.setText( unicode( self._contents.get( item, '' ) ) )


  def refresh( self ):
    try:
      reader = neuroLog.LogFileReader( self._fileName )
    except IOError:
      neuroException.showException()
      reader = None
    # Save list state
    itemIndex = 0
    currentItemIndex = -1
    listState = []
    it = QTreeWidgetItemIterator(self._list)
    while it.value():
      currentItem=it.value()
      listState.append( currentItem.isExpanded(  ) )
      if self._list.currentItem() is currentItem:
        currentItemIndex = itemIndex
      it+=1
      itemIndex+=1      
    # Erase list
    self._list.clear()
    self._contents = {}
    # Reread log and restore list state
    after=None
    currentItemList = []
    if reader is not None:
      itemIndex = -1
      item = reader.readItem()
      while item is not None:
        ( after, itemIndex ) = self._addLogItem( item, self._list, after, itemIndex, listState, currentItemIndex, currentItemList )
        item = reader.readItem()
    if currentItemList:
      self._list.setCurrentItem( currentItemList[ 0 ] )
    self._list.resizeColumnToContents(0)


  def open( self ):
     #QFileDialog.getOpenFileName( QWidget * parent = 0, const QString & caption = QString(), const QString & dir = QString(), const QString & filter = QString(), QString * selectedFilter = 0, Options options = 0)
    logFileName = unicode( QFileDialog.getOpenFileName( None, _t_( 'Open log file' ), self._fileName, '', None, QFileDialog.DontUseNativeDialog ) )
    if logFileName:
      self.setLogFile( logFileName )

  #def keyPressEvent( self, e ):
    #if e.state() == Qt.ControlButton and e.key() == Qt.Key_W:
      #e.accept()
      #self.close()
    #else:
      #e.ignore()
      #QWidget.keyPressEvent( self, e )

def showLog( fileName ):
  l = LogViewer( fileName )
  l.show()
