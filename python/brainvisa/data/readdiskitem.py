import sys, os, operator
from itertools import chain

from qt import QHBox, QLineEdit, SIGNAL, PYSIGNAL, QToolTip, QPushButton, \
               Qt, QPixmap, QImage, QWidget, QFileDialog, QStringList, QVBoxLayout, \
               QListBox, QHBoxLayout, QSpacerItem, QSizePolicy

from soma.debug import print_stack
from soma.wip.application.api import findIconFile
from soma.qt3gui.api import largeIconSize
from soma.undefined import Undefined

from brainvisa.data.diskItemBrowser import DiskItemBrowser, diskItemFilter
from neuroData import Parameter
from neuroDataGUI import DataEditor, StringListEditor
from neuroProcesses import getDiskItemType, defaultContext
import neuroProcesses
import neuroProcessesGUI
from neuroDiskItems import getFormat, getFormats, DiskItem, isSameDiskItemType, Directory
import neuroHierarchy, neuroConfig
from neuroException import showException, HTMLMessage

#----------------------------------------------------------------------------
class RightClickablePushButton( QPushButton ):
  def mousePressEvent( self, e ):
    if e.button() == Qt.RightButton:
      self.emit( PYSIGNAL( 'rightPressed' ), () )
    else:
      QPushButton.mousePressEvent( self, e )


#----------------------------------------------------------------------------
class FileDialog( QFileDialog ):
  def __init__( self, parent ):
    QFileDialog.__init__( self, parent )

  def accept( self ):
    QFileDialog.accept( self )
    self.emit( PYSIGNAL( 'accept' ), () )


#----------------------------------------------------------------------------
class DiskItemEditor( QHBox, DataEditor ):
  def __init__( self, parameter, parent, name, write = False, context = None ):
    if getattr( DiskItemEditor, 'pixShow', None ) is None:
      setattr( DiskItemEditor, 'pixShow', QPixmap( QImage(findIconFile( 'eye.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemEditor, 'pixEdit', QPixmap( QImage(findIconFile( 'pencil.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemEditor, 'pixDatabaseRead', QPixmap( QImage(findIconFile( 'database_read.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemEditor, 'pixDatabaseWrite', QPixmap( QImage(findIconFile( 'database_write.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemEditor, 'pixBrowseRead', QPixmap( QImage(findIconFile( 'browse_read.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemEditor, 'pixBrowseWrite', QPixmap( QImage(findIconFile( 'browse_write.png' )).smoothScale(*largeIconSize) ) )
    QHBox.__init__( self, parent, name )
    self.setSpacing( 4 )
    self._write = write
    self.parameter =  parameter
    self.led = QLineEdit( self )
    self.connect( self.led, SIGNAL( 'textChanged( const QString & )' ), self.textChanged )
    self.connect( self.led, SIGNAL( 'returnPressed()' ), self.checkValue )
    self.setFocusProxy( self.led )
    self.diskItem = None
    self.forceDefault = False
    self._context = context

    self.btnShow = RightClickablePushButton( self )
    self.btnShow.setPixmap( self.pixShow )
    self.btnShow.setToggleButton( True )
    self.btnShow.setFocusPolicy( QWidget.NoFocus )
    self.btnShow.setEnabled( False )
    if not neuroProcesses.getViewer( (self.parameter.type, self.parameter.formats[0] ), 1 ):
      self.btnShow.hide()
    self._view = None
    self.connect( self.btnShow, SIGNAL( 'clicked()' ), self.showPressed )
    self.connect( self.btnShow, PYSIGNAL( 'rightPressed' ), self.openViewerPressed )
    self._edit = None
    self.btnEdit = RightClickablePushButton( self )
    self.btnEdit.setPixmap( self.pixEdit )
    self.btnEdit.setToggleButton( 1 )
    self.btnEdit.setFocusPolicy( QWidget.NoFocus )
    self.btnEdit.setEnabled( 0 )
    if not neuroProcesses.getDataEditor( (self.parameter.type, self.parameter.formats[0] ) ):
      self.btnEdit.hide()
    self.connect( self.btnEdit, SIGNAL( 'clicked()' ), self.editPressed )
    self.connect( self.btnEdit, PYSIGNAL( 'rightPressed' ),
                  self.openEditorPressed )
    self.btnDatabase = QPushButton( self )
    if write:
      self.btnDatabase.setPixmap( self.pixDatabaseWrite )
      QToolTip.add(self.btnDatabase,_t_("Browse the database (save mode)"))
    else:
      self.btnDatabase.setPixmap( self.pixDatabaseRead )
      QToolTip.add(self.btnDatabase,_t_("Browse the database (load mode)"))
    self.btnDatabase.setFocusPolicy( QWidget.NoFocus )
    self.connect( self.btnDatabase, SIGNAL( 'clicked()' ), self.databasePressed )
    self.databaseDialog = None
    self.btnBrowse = QPushButton( self )
    if write:
      self.btnBrowse.setPixmap( self.pixBrowseWrite )
      QToolTip.add(self.btnBrowse,_t_("Browse the filesystem (save mode)"))
    else:
      self.btnBrowse.setPixmap( self.pixBrowseRead )
      QToolTip.add(self.btnBrowse,_t_("Browse the filesystem (load mode)"))
    self.btnBrowse.setFocusPolicy( QWidget.NoFocus )
    self.connect( self.btnBrowse, SIGNAL( 'clicked()' ), self.browsePressed )
    self.browseDialog = None
    self._textChanged = False

    self._selectedAttributes = {}
    self.parameter.valueLinkedNotifier.add( self.valueLinked )

  def __del__( self ):
      self._ = None
        
  def setContext( self, newContext ):
    oldContext = ( self.btnShow.isOn(), self._view,
                   self.btnEdit.isOn(), self._edit )
    if newContext is None:
      self.btnShow.setOn( False )
      self.btnEdit.setOn( False )
      self._view = None
      self._edit = None
    else:
      if len( newContext ) >=4:
        o, v, z, e = newContext
      else:
        o, v = newContext
        z = e = 0
      self.btnShow.setOn( o )
      self._view = v
      self.btnEdit.setOn( z )
      self._edit = e
    return oldContext
  

  def getValue( self ):
    return self.diskItem
    

  def setValue( self, value, default = 0 ):
    self.forceDefault = default
    self.diskItem = self.parameter.findValue( value )
    if self.diskItem is None:
      if value is None: self.led.setText( '' )
      if self.btnShow: self.btnShow.setEnabled( 0 )
      if self.btnEdit: self.btnEdit.setEnabled( 0 )
      self.emit( PYSIGNAL('newValidValue'), ( self.name(), self.diskItem, ) )
    else:
      self.led.setText( self.diskItem.fullPath() )
      self.checkReadable()
      self.emit( PYSIGNAL('newValidValue'), ( self.name(), self.diskItem, ) )
    self._textChanged = 0
    self.forceDefault = 0

  def checkReadable( self ):
    if self.btnShow:
      enabled = 0
      v = neuroProcesses.getViewer( (self.parameter.type, self.parameter.formats[0] ), 1 )
      if v:
        self.btnShow.show()
      else:
        self.btnShow.hide()
      if self.diskItem:
        if v:
          enabled = self.diskItem.isReadable()
      self.btnShow.setEnabled( enabled )
    if self.btnEdit:
      enabled = 0
      v = neuroProcesses.getDataEditor( (self.parameter.type, self.parameter.formats[0] ) )
      if v:
        self.btnEdit.show()
      else:
        self.btnEdit.hide()
      if self.diskItem:
        if v:
          enabled = self.diskItem.isWriteable()
      self.btnEdit.setEnabled( enabled )

  def textChanged( self ):
    self._textChanged = 1
    if not self.forceDefault:
      self.emit( PYSIGNAL('noDefault'), ( self.name(),) )

  def checkValue( self ):
    if self._textChanged:
      self.setValue( unicode( self.led.text() ) )
  
  def showPressed( self ):
    if self.btnShow.isOn():
      self.btnShow.setEnabled( 0 )
      v = self.getValue()
      try :
        viewer = neuroProcesses.getViewer( v, 1 )()
        defaultContext().runInteractiveProcess( self._viewerExited, viewer, v )
      except Exception, error :
        raise RuntimeError( HTMLMessage(_t_( 'No viewer could be found or launched for type =<em>%s</em> and format=<em>%s</em>' ) % (unicode( v.type ), unicode(v.format))) )
    else:
      self._view = None


  def _viewerExited( self, result ):
    if isinstance( result, Exception ):
      showException( parent=self )
    else:
      self._view = result
    neuroProcessesGUI.mainThreadActions().push( self.btnShow.setEnabled, 1 )


  def openViewerPressed( self ):
    v = self.getValue()
    viewer = neuroProcesses.getViewer( v, 1 )()
    neuroProcessesGUI.showProcess( viewer, v )

  
  def editPressed( self ):
    if self.btnEdit.isOn():
      self.btnEdit.setEnabled( 0 )
      v = self.getValue()
      editor = neuroProcesses.getDataEditor( v )()
      defaultContext().runInteractiveProcess( self._editorExited, editor, v )
    else:
      self._edit = None
  
  
  def _editorExited( self, result ):
    if isinstance( result, Exception ):
      showException( parent=self )
    else:
      self._edit = result
    neuroProcessesGUI.mainThreadActions().push( self.btnEdit.setEnabled, True )
    neuroProcessesGUI.mainThreadActions().push( self.btnEdit.setOn, False )

  
  def openEditorPressed( self ):
    v = self.getValue()
    editor = neuroProcesses.getDataEditor( v )()
    neuroProcessesGUI.showProcess( editor, v )


  def databasePressed( self ):
    if self.databaseDialog is None or self.parameter._modified:
      self.parameter._modified = 0
      if self.diskItem: # this parameter has already a value, use it to initialize the browser
        selection = self.diskItem.hierarchyAttributes()
        if self.diskItem.type is None :
          selection[ '_type' ] = None
        else :
          selection[ '_type' ] = self.diskItem.type.name
        if self.diskItem.format is None :
          selection[ '_format' ] = None
        else :
          selection[ '_format' ] = self.diskItem.format.name
        
        self.databaseDialog = DiskItemBrowser( self.parameter.database, selection=selection, required=self.parameter.requiredAttributes, parent=self, write = self._write,
        enableConversion=self.parameter.enableConversion )
      else: # if there is no value, we could have some selected attributes from a linked value, use it to initialize the browser
        self.databaseDialog = DiskItemBrowser( self.parameter.database, selection=self._selectedAttributes, required=self.parameter.requiredAttributes, parent=self, write = self._write, enableConversion=self.parameter.enableConversion )
      self.databaseDialog.setCaption( _t_( self.parameter.type.name ) )
      self.connect( self.databaseDialog, PYSIGNAL( 'accept' ), self.databaseAccepted )
    else:
      if self.diskItem:
        self.databaseDialog.resetSelectedAttributes( self.diskItem )
        #self.databaseDialog.rescan(selectedType=self.diskItem.type, selectedFormat=self.diskItem.format, selectedAttributes=self.diskItem.hierarchyAttributes())
      else:
        self.databaseDialog.resetSelectedAttributes( self._selectedAttributes )
       #self.databaseDialog.rescan( selectedAttributes=self._selectedAttributes)
    self.databaseDialog.show()

  def databaseAccepted( self ):
    values=self.databaseDialog.getValues()
    if values:
      self.setValue( values[0] )

  def browsePressed( self ):
    if self.browseDialog is None or self.parameter._modified:
      self.parameter._modified = False
      self.browseDialog = FileDialog( self )
      if self._write:
        mode = QFileDialog.AnyFile
      else:
        mode = QFileDialog.ExistingFile
      filters = QStringList()
      allPatterns = {}
      dirOnly = True
      for f in self.parameter.formats:
        if f.fileOrDirectory() is not Directory:
          dirOnly = False
        flt = f.getPatterns().unmatch( {}, { 'filename_variable': '*' } )[ 0 ]
        allPatterns[ flt ] = 1
        filters.append( _t_( f.name ) + ' (' + flt + ')' )
      filters.prepend( _t_( 'Recognized formats' ) + ' (' \
        + ';'.join( allPatterns.keys() ) + ')' )
      filters.append( _t_( 'All files' ) + ' (*)' )
      if dirOnly:
        mode = QFileDialog.Directory
      self.browseDialog.setMode( mode )
      self.browseDialog.setFilters( filters )
      self.connect( self.browseDialog, PYSIGNAL( 'accept' ), self.browseAccepted )
    # set current directory
    parent = self._context
    if hasattr( parent, '_currentDirectory' ) and parent._currentDirectory:
      self.browseDialog.setDir( parent._currentDirectory )
    self.browseDialog.show()

  def browseAccepted( self ):
    value = unicode( self.browseDialog.selectedFile() )
    parent = self._context
    if hasattr( parent, '_currentDirectory' ):
      parent._currentDirectory = unicode( self.browseDialog.dirPath() )
    self.setValue( unicode( self.browseDialog.selectedFile() ) )
    
  def valueLinked( self, parameterized, name, value ):
    if isinstance( value, DiskItem ):
      self._selectedAttributes = value.hierarchyAttributes()

  def releaseCallbacks( self ):
    self._view = None
    self._edit = None



#----------------------------------------------------------------------------
class DiskItemListEditor( QHBox, DataEditor ):
  
  class DiskItemListSelect( QWidget ): # Ex QSemiModal


    def __init__( self, dilEditor, name, write, context = None ):
      self._context = context
      if getattr( DiskItemListEditor.DiskItemListSelect, 'pixUp', None ) is None:
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixUp', 
          QPixmap( QImage(findIconFile( 'up.png' )).smoothScale(*largeIconSize) ) )
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixDown', 
          QPixmap( QImage(findIconFile( 'down.png' )).smoothScale(*largeIconSize) ) )
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixFindRead', 
          QPixmap( QImage(findIconFile( 'database_read.png' )).smoothScale(*largeIconSize) ) )
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixFindWrite', 
          QPixmap( QImage(findIconFile( 'database_write.png' )).smoothScale(*largeIconSize) ) )
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixBrowseRead', 
          QPixmap( QImage(findIconFile( 'browse_read.png' )).smoothScale(*largeIconSize) ) )
        setattr( DiskItemListEditor.DiskItemListSelect, 'pixBrowseWrite', 
          QPixmap( QImage(findIconFile( 'browse_write.png' )).smoothScale(*largeIconSize) ) )
      QWidget.__init__( self, dilEditor.topLevelWidget(), name,
        Qt.WType_Dialog + Qt.WGroupLeader + Qt.WStyle_StaysOnTop + Qt.WShowModal )
      layout = QVBoxLayout( self )
      layout.setMargin( 10 )
      layout.setSpacing( 5 )
      
      self.dilEditor = dilEditor
      self.parameter = dilEditor.parameter
      self.values = []
      self.browseDialog = None
      self.findDialog = None
      
      self.lbxValues = QListBox( self )
      self.connect( self.lbxValues, SIGNAL('currentChanged( QListBoxItem * )'), self._currentChanged )
      layout.addWidget( self.lbxValues )

      hb = QHBoxLayout()
      hb.setSpacing( 6 )
      
      self.btnAdd = QPushButton( _t_( 'Add' ), self )
      self.connect( self.btnAdd, SIGNAL( 'clicked()' ), self._add )
      hb.addWidget( self.btnAdd )

      self.btnRemove = QPushButton( _t_( 'Remove' ), self )
      self.btnRemove.setEnabled( 0 )
      self.connect( self.btnRemove, SIGNAL( 'clicked()' ), self._remove )
      hb.addWidget( self.btnRemove )
      
      self.btnUp = QPushButton( self )
      self.btnUp.setPixmap( self.pixUp )
      self.btnUp.setEnabled( 0 )
      self.connect( self.btnUp, SIGNAL( 'clicked()' ), self._up )
      hb.addWidget( self.btnUp )

      self.btnDown = QPushButton( self )
      self.btnDown.setPixmap( self.pixDown )
      self.btnDown.setEnabled( 0 )
      self.connect( self.btnDown, SIGNAL( 'clicked()' ), self._down )
      hb.addWidget( self.btnDown )

      spacer = QSpacerItem( 10, 10, QSizePolicy.Expanding, QSizePolicy.Minimum )
      hb.addItem( spacer )

      layout.addLayout( hb )

      hb = QHBoxLayout()
      hb.setSpacing( 6 )

      self.sle = StringListEditor( self, self.name() )
      hb.addWidget( self.sle )

      btn = QPushButton( self )
      if write:
        btn.setPixmap( self.pixFindWrite )
      else:
        btn.setPixmap( self.pixFindRead )
      self.connect( btn, SIGNAL( 'clicked()' ), self.findPressed )
      hb.addWidget( btn )
      
      btn = QPushButton( self )
      if write:
        btn.setPixmap( self.pixBrowseWrite )
      else:
        btn.setPixmap( self.pixBrowseRead )
      self.connect( btn, SIGNAL( 'clicked()' ), self.browsePressed )
      hb.addWidget( btn )
      
      layout.addLayout( hb )
            
#      self.editor = self.parameter.editor( self, self.name() )
#      layout.addWidget( self.editor )
      
      hb = QHBoxLayout()
      hb.setSpacing(6)
      hb.setMargin(6)
      spacer = QSpacerItem(20,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
      hb.addItem( spacer )
      btn =QPushButton( _t_('Ok'), self )
      hb.addWidget( btn )
      self.connect( btn, SIGNAL( 'clicked()' ), self._ok )
      btn =QPushButton( _t_('Cancel'), self )
      hb.addWidget( btn )
      self.connect( btn, SIGNAL( 'clicked()' ), self._cancel )
      layout.addLayout( hb )

      neuroConfig.registerObject( self )

    def close( self, alsoDelete ):
      neuroConfig.unregisterObject( self )
      return QWidget.close( self, alsoDelete )
      
    def _currentChanged( self ):
      index = self.lbxValues.currentItem()
      if index >= 0 and index < len( self.values ):
        self.sle.setValue( [ self.values[ index ].fullPath() ] )
        self.btnRemove.setEnabled( 1 )
        if index > 0:
          self.btnUp.setEnabled( 1 )
        else:
          self.btnUp.setEnabled( 0 )
        if index < ( len( self.values ) - 1 ):
          self.btnDown.setEnabled( 1 )
        else:
          self.btnDown.setEnabled( 0 )
      else:
        self.sle.setValue( None )
        self.btnRemove.setEnabled( 0 )
        self.btnUp.setEnabled( 0 )
        self.btnDown.setEnabled( 0 )

    def _add( self ):
      try:
        for v in map( self.parameter.findValue, self.sle.getValue() ):
          self.values.append( v )
          if v is None:
            self.lbxValues.insertItem( '<' + _t_('None') + '>' )
          else:
            self.lbxValues.insertItem( v.fileName() )
        self.lbxValues.setCurrentItem( len( self.values ) - 1 )   
      except:
        showException( parent=self )
    
    def _remove( self ):
      index = self.lbxValues.currentItem()
      del self.values[ index ]
      self.lbxValues.removeItem( index )
      
    def _up( self ):
      index = self.lbxValues.currentItem()
      tmp = self.values[ index ]
      self.values[ index ] = self.values[ index - 1 ]
      self.values[ index - 1 ] = tmp
      tmp = self.lbxValues.text( index )
      self.lbxValues.changeItem( self.lbxValues.text( index - 1 ), index )
      self.lbxValues.changeItem( tmp, index - 1 )
      
    def _down( self ):
      index = self.lbxValues.currentItem()
      tmp = self.values[ index ]
      self.values[ index ] = self.values[ index + 1 ]
      self.values[ index + 1 ] = tmp
      tmp = self.lbxValues.text( index )
      self.lbxValues.changeItem( self.lbxValues.text( index + 1 ), index )
      self.lbxValues.changeItem( tmp, index + 1 )
    
    def setValue( self, value ):
      if isinstance( value, ( list, tuple ) ):
        self.values = []
        self.lbxValues.clear()
        for v in value:
          self.values.append( v )
          if v is None:
            self.lbxValues.insertItem( '<' + _t_('None') + '>' )
          else:
            self.lbxValues.insertItem( v.fileName() )
      
    def _ok( self ):
      self.dilEditor._newValue( self.values )
      self.close( 1 )
      
    def _cancel( self ):
      self.close( 1 )

    def findPressed( self ):
      if self.findDialog is None:
        self.findDialog = DiskItemBrowser( self.parameter.database, 
          required=self.parameter.requiredAttributes, 
          parent=self,
          write = self.parameter._write,
          enableConversion=self.parameter.enableConversion,
          multiple = True )
        self.connect( self.findDialog, PYSIGNAL( 'accept' ), self.findAccepted )
      else:
        self.findDialog.rescan()
      self.findDialog.show()

    def findAccepted( self ):
      value = map( lambda x: x.fullPath(), self.findDialog.getValues() )
      if self.isVisible():
        self.sle.setValue( value )
        self._add()
      else:
        self.emit( PYSIGNAL( 'accept' ), ( value, ) )

    def browsePressed( self ):
      if self.browseDialog is None:
        self.browseDialog = FileDialog( self.topLevelWidget() )
        self.browseDialog.setMode( self.browseDialog.ExistingFiles )
        filters = QStringList()
        allPatterns = {}
        dirOnly = 1
        for f in self.parameter.formats:
          if f.fileOrDirectory() is not Directory:
            dirOnly = 0
          flt = f.getPatterns().unmatch( {}, { 'filename_variable': '*' } )[ 0 ]
          allPatterns[ flt ] = 1
          filters.append( _t_( f.name ) + ' (' + flt + ')' )
        filters.prepend( _t_( 'Recognized formats' ) + ' (' \
          + ';'.join( allPatterns.keys() ) + ')' )
        filters.append( _t_( 'All files' ) + ' (*)' )
        self.browseDialog.setFilters( filters )
        # self.connect( self.browseDialog, SIGNAL( 'fileSelected( const QString & )' ), self.browseAccepted )
        self.connect( self.browseDialog, PYSIGNAL( 'accept' ), self.browseAccepted )
        if dirOnly:
          self.browseDialog.setMode( self.browseDialog.Directory )
        parent = self._context
        if hasattr( parent, '_currentDirectory' ) and parent._currentDirectory:
          self.browseDialog.setDir( parent._currentDirectory )
      self.browseDialog.show()

    def browseAccepted( self ):
      parent = self._context
      if hasattr( parent, '_currentDirectory' ):
        parent._currentDirectory = unicode( self.browseDialog.dirPath() )
      l = [str(i) for i in self.browseDialog.selectedFiles()]
      if self.isVisible():
        self.sle.setValue( l ) 
        self._add()
      else:
        self.emit( PYSIGNAL( 'accept' ), ( l, ) )


  def __init__( self, parameter, parent, name, write = 0, context=None ):
    if getattr( DiskItemListEditor, 'pixFindRead', None ) is None:
      setattr( DiskItemListEditor, 'pixFindRead', QPixmap( QImage(findIconFile( 'database_read.png' )).smoothScale(*largeIconSize) ) )
      setattr( DiskItemListEditor, 'pixFindWrite', QPixmap( QImage(findIconFile( 'database_write.png' )).smoothScale(*largeIconSize)  ) )
      setattr( DiskItemListEditor, 'pixBrowseRead', QPixmap( QImage(findIconFile( 'browse_read.png' )).smoothScale(*largeIconSize)  ) )
      setattr( DiskItemListEditor, 'pixBrowseWrite', QPixmap( QImage(findIconFile( 'browse_write.png' )).smoothScale(*largeIconSize)  ) )
    QHBox.__init__( self, parent, name )
    self._context = context
    self.parameter = parameter
    self.write = write
    self.sle = StringListEditor( self, name )
    self._value = None
    self.connect( self.sle, PYSIGNAL( 'newValidValue' ), self._newTextValue )

    self.btnFind = RightClickablePushButton( self )
    if write:
      self.btnFind.setPixmap( self.pixFindWrite )
    else:
      self.btnFind.setPixmap( self.pixFindRead )
    self.btnFind.setFocusPolicy( QWidget.NoFocus )
    self.connect( self.btnFind, SIGNAL( 'clicked()' ), self.findPressed )
    self.connect( self.btnFind, PYSIGNAL( 'rightPressed' ), self.findRightPressed )
    self.btnBrowse = RightClickablePushButton( self )
    if write:
      self.btnBrowse.setPixmap( self.pixBrowseWrite )
    else:
      self.btnBrowse.setPixmap( self.pixBrowseRead )
    self.btnBrowse.setFocusPolicy( QWidget.NoFocus )
    self.connect( self.btnBrowse, SIGNAL( 'clicked()' ), self.browsePressed )
    self.connect( self.btnBrowse, PYSIGNAL( 'rightPressed' ), self.browseRightPressed )

    self.setValue( None, 1 )
    
  def getValue( self ):
    return self._value
    
  def setValue( self, value, default = 0 ):
    self.forceDefault = default
    self._value = value
    if isinstance( value, ( list, tuple ) ):
      r = []
      for v in value:
        if v is None:
          r.append( '' )
        else:
          r.append( str( v ) )
      value = r
    self.sle.setValue( value, default )
    self.forceDefault = 0
  
  def findPressed( self ):
    w = self.DiskItemListSelect( self, self.name(), self.write )
    try:
      w.setValue( self.getValue() )
    except:
      showException( parent = self )
    self.connect( w, PYSIGNAL( 'accept' ), self._newValue )
    w.findPressed()
  
  def findRightPressed( self ):
    w = self.DiskItemListSelect( self, self.name(), self.write )
    try:
      w.setValue( self.getValue() )
    except:
      showException( parent = self )
    w.show()
    w.findPressed()

  def browsePressed( self ):
    w = self.DiskItemListSelect( self, self.name(), self.write, 
                                context = self._context )
    try:
      w.setValue( self.getValue() )
    except:
      showException( parent = self )
    self.connect( w, PYSIGNAL( 'accept' ), self._newValue )
    w.browsePressed()
    
  def browseRightPressed( self ):
    w = self.DiskItemListSelect( self, self.name(), self.write,
                                  context = self._context )
    try:
      w.setValue( self.getValue() )
    except:
      showException( parent = self )
    w.show()
    w.browsePressed()

  def _newTextValue( self ):
    textValues = self.sle.getValue()
    if textValues is not None:
      self._newValue( [self.parameter.findValue( x ) for x in textValues] )
    return None

  def _newValue( self, v ):
    self.setValue( v )
    self.emit( PYSIGNAL('newValidValue'), ( self.name(), v, ) )
    if not self.forceDefault: self.emit( PYSIGNAL('noDefault'), ( self.name(),) )

  def checkValue( self ):
    self.sle.checkValue()


#------------------------------------------------------------------------------
def diskItemDistance( diskItem, other ):
  '''Returns a value that represents a sort of distance between two DiskItems.
      The distance is not a number but distances can be sorted.'''
  # Count the number of common hierarchy attributes
  if isinstance( other, DiskItem ):
    if other.type is not None:
      other_type = other.type.name
    else:
      other_type = None
    other_priority = other.priority()
  else:
    other_type = other.get( '_type' )
    other_priority = 0
  if diskItem.type is not None:
    diskItem_type = diskItem.type.name
  else:
    diskItem_type = None
  hierarchyCommon = reduce( operator.add, 
      ( (other.get( n ) == v) for n, v in diskItem.hierarchyAttributes().iteritems() ),
      ( int(diskItem_type==other_type) ) )
  # Count the number of common non hierarchy attributes
  nonHierarchyCommon = reduce( operator.add, 
      ( (other.get( n ) == v) for n, v in diskItem.nonHierarchyAttributes().iteritems() ),
      (int(diskItem_type == other_type) ) )
  return ( -hierarchyCommon, other_priority - diskItem.priority(), -nonHierarchyCommon  )


#----------------------------------------------------------------------------
class ReadDiskItem( Parameter ):
  def __init__( self, diskItemType, formats, requiredAttributes={},
                enableConversion=True, ignoreAttributes=False, _debug=None ):
    Parameter.__init__( self )
    self._debug = _debug
    self._formatsWithConverter = None
    self.type = getDiskItemType( diskItemType )
    self.formats = getFormats( formats )
    self.enableConversion = enableConversion
    self.requiredAttributes = requiredAttributes
    self._write = False
    #self._modified = 0
    self.ignoreAttributes = ignoreAttributes;
  
  
  def _getDatabase( self ):
    return neuroHierarchy.databases
  database = property( _getDatabase )
  
  
  # Allow direct affectation to requiredAttributes for backward compatibility
  def _getRequiredAttributes( self ):
    if self._formatsWithConverter is None:
      self._formatsWithConverter = {}
      self.requiredAttributes = self.requiredAttributes
    return self._requiredAttributes
    
  def _setRequiredAttributes( self, value ):
    self._requiredAttributes = value.copy()
    self._requiredAttributes[ '_type' ] = self.type.name
    formats = set( self.database.formats.getFormat( f.name, f ).name for f in self.formats )
    #if self._debug is not None:
      #print >> self._debug, '!_setRequiredAttributes!', self, self.type, 'formats', [f for f in self.formats]
    if self.enableConversion and self._formatsWithConverter is not None:
      self._formatsWithConverter = {}
      #if self._debug is not None:
        #print >> self._debug, '!_setRequiredAttributes!', self, self.type, 'conversion enabled'
      any = getDiskItemType( 'Any type' )
      for f in self.formats:
        #if self._debug is not None:
          #print >> self._debug, '!_setRequiredAttributes!', self, self.type, 'Examining converters to', f, '(' + repr( type(f) ) + ')', len( neuroProcesses._converters )
        for type_format, converter in neuroProcesses.getConvertersTo( ( any, f ) ).iteritems():
          typ, format = type_format
          formatName = self.database.formats.getFormat( format.name, format ).name
          #if self._debug is not None:
            #print >> self._debug, '!_setRequiredAttributes!', self, self.type, '  <--', formatName
          if formatName not in formats:
            self._formatsWithConverter[ formatName ] = converter
      formats.update( self._formatsWithConverter.iterkeys() )
    #elif self._debug is not None:
      #print >> self._debug, '!_setRequiredAttributes!', self, self.type, 'conversion disabled'
    self._requiredAttributes[ '_format' ] = formats
    #if self._debug is not None:
      #if self._formatsWithConverter:
        #print >> self._debug, '!_setRequiredAttributes!', self, self.type, '_formatsWithConverter', self._formatsWithConverter
      #print >> self._debug, '!_setRequiredAttributes!', self, self.type, '_requiredAttributes', self._requiredAttributes
  requiredAttributes = property( _getRequiredAttributes, _setRequiredAttributes )
  
  
  #def _getFormats( self ):
    #return self._formats
  #def _setFormats( self, formats ):
    #self._formats = getFormats( formats )
  #format = property( _getFormats, _setFormats )
  
  
  def checkValue( self, name, value ):
    Parameter.checkValue( self, name, value )
    if value is not None:
      if not value.isReadable():
        raise RuntimeError( HTMLMessage(_t_( '<em>%s</em> is not readable or does not exists' ) % unicode( value )) )


  def findValue( self, selection, requiredAttributes=None, _debug=Undefined ):
    if _debug is Undefined:
      _debug = self._debug
    if selection is None: return None
    if requiredAttributes is None:
      requiredAttributes = self.requiredAttributes
    else:
      r = self.requiredAttributes.copy()
      r.update( requiredAttributes )
      requiredAttributes = r
    if _debug is not None:
       print >> _debug, '\n' + '-' * 70
       print >> _debug, self.__class__.__name__ + '(\'' + str( self.type ) + '\').findValue'
       if isinstance( selection, DiskItem ):
         print >> _debug, '  value type = DiskItem'
         print >> _debug, '  fullPath = ', repr( selection )
         print >> _debug, '  type = ', repr( selection.type )
         print >> _debug, '  format = ', repr( selection.format )
         print >> _debug, '  attributes:'
         for n, v in selection.attributes().items():
           print >> _debug, '   ', n, '=', repr( v )
       else:
         print >> _debug, '  value type =', type( selection )
         print >> _debug, '  value = ', selection
       print >> _debug, '  required attributes:'
       for n, v in requiredAttributes.iteritems():
         print >> _debug, '   ', n, '=', repr( v )
       #print >> _debug, '- ' * 35
       #print_stack( file=_debug )
       #print >> _debug, '- ' * 35
    
    result = None
    fullSelection = None
    write = False
    if isinstance( selection, DiskItem ):
      if selection.getHierarchy( '_database' ) is None:
        # If DiskItem is not in a database, required attributes are ignored (except _format)
        rr = {}
        if requiredAttributes.has_key( '_format' ):
          rr[ '_format' ] = requiredAttributes[ '_format' ]
        requiredAttributes = rr
        
      if ( selection.type is None or isSameDiskItemType( selection.type, self.type ) or isSameDiskItemType( self.type, selection.type )) \
        and self.diskItemFilter( selection, requiredAttributes ):
          result = selection
          
      else:
        if _debug is not None:
          print >> _debug, '  DiskItem rejected because:', self.diskItemFilter( selection, requiredAttributes, explainRejection=True )
        if selection._write:
          if _debug is not None:
            print >> _debug, '  DiskItem has the _write attribute set to True'
          write = True
        fullSelection = selection.globalAttributes()
        if selection.type is None :
          fullSelection[ '_type' ] = None
        else :
          fullSelection[ '_type' ] = selection.type.name
          
        if selection.format is None :
          fullSelection[ '_format' ] = None
        else :
          fullSelection[ '_format' ] = selection.format.name
          
    elif isinstance( selection, basestring ):
      fullselection = None
      fileName = os.path.normpath( os.path.abspath( selection ) )
      result = self.database.getDiskItemFromFileName( fileName, None )
      if result is None:
        if _debug is not None:
          print >> _debug, '  DiskItem not found in databases'
        result = self.database.createDiskItemFromFileName( fileName, None )
        if result is None:
          if _debug is not None:
            print >> _debug, '  DiskItem not created in databases'
          result = self.database.createDiskItemFromFormatExtension( fileName, None )
          if result is not None:
            result.readAndUpdateMinf()
        else:
          result.readAndUpdateMinf()
          if _debug is not None:
            print >> _debug, '  DiskItem created in databases'
        if result is None:
          if _debug is not None:
            print >> _debug, '  no format found for file name extension'
          directoryFormat = getFormat( 'Directory' )
          if directoryFormat in self.formats:
            # In this case, item is a directory
            if _debug is not None:
              print >> _debug, '  no extension ==> create Directory item'
            result = Directory( fileName, None )
            result.format = directoryFormat
            result._files = [ fileName ]
            result._identified = False
            result.type = self.type
            result.readAndUpdateMinf()
        elif _debug is not None:
          print >> _debug, '  found matching format:', result.format
      elif _debug is not None:
        print >> _debug, '  DiskItem found in databases'
      if result is not None:
        if result.type is None :
          # Set the result type if this is not already done
          result.type=self.type
        if result.getHierarchy( '_database' ) is None:
          # If DiskItem is not in a database, required attributes are ignored
          requiredAttributes = { '_format': requiredAttributes.get( '_format' ) }
          # if the diskitem format does not match the required format, and if required format contains Series of diskitem.format, try to change the format of the diskItem to a format series.
          if not self.diskItemFilter( result, requiredAttributes ):
            if ("Series of "+result.format.name in requiredAttributes.get("_format") ):
              self.database.changeDiskItemFormatToSeries(result)
        if not self.diskItemFilter( result, requiredAttributes ):
          if _debug is not None:
            print >> _debug, '  DiskItem rejected because:', self.diskItemFilter( result, requiredAttributes, explainRejection=True )
          result = None
    elif isinstance( selection, dict ):
      fullSelection = dict( selection )
      
    if result is None and fullSelection is not None:
      values = list( self._findValues( fullSelection, requiredAttributes, write=(write or self._write) , _debug=_debug ) )

      if values:
        if len( values ) == 1:
          result = values[ 0 ]
        else:
          # Find the item with the "smallest" "distance" from item
          values = sorted( (diskItemDistance( i, selection ), i ) for i in values )
          if _debug is not None:
            print >> _debug, '  findValue priority sorted items:'
            for l in values:
              print >> _debug, '   ', l
          if values[ 0 ][ 0 ] != values[ 1 ] [ 0 ]:
            result = values[ 0 ][ 1 ]
    if _debug is not None:
      print >> _debug, '-> findValue return', result
      if result is not None:
        print >> _debug, '-> type:', result.type
        print >> _debug, '-> format:', result.format
        print >> _debug, '-> attributes:'
        for n, v in result.attributes().items():
          print >> _debug, '->  ', n, '=', v
      print >> _debug, '-' * 70 + '\n'
      _debug.flush()
    return result
  
  
  def _findValues( self, selection, requiredAttributes, write, _debug=Undefined ):
    if _debug is Undefined:
      _debug = self._debug
    if requiredAttributes is None:
      requiredAttributes = self.requiredAttributes
    keySelection={}
    if selection:
      # in selection attributes, choose only key attributes because the request must not be too restrictive to avoid failure. The results will be sorted by distance to the selection later.
      keyAttributes=self.database.getTypesKeysAttributes(self.type.name)
      keySelection=dict( (i,selection[i] ) for i in keyAttributes if i in selection )
    readValues = ( i for i in self.database.findDiskItems( keySelection, _debug=_debug, **requiredAttributes ) if self.diskItemFilter( i, requiredAttributes ) )
    if write:
      # use selection attributes to create a new diskitem
      fullPaths = set()
      for item in readValues:
        fullPaths.add( item.fullPath() )
        yield item
      if self._formatsWithConverter:
        # Do not allow formats that require a conversion in DiskItem creation
        oldFormats = requiredAttributes.get( '_format' )
        requiredAttributes[ '_format' ] = oldFormats.difference( self._formatsWithConverter.iterkeys() )
      for item in self.database.createDiskItems( selection, _debug=_debug, **requiredAttributes ):
        if self.diskItemFilter( item, requiredAttributes ):
          if item.fullPath() not in fullPaths:
            yield item
        elif _debug is not None:
          print >> _debug, ' ', item, 'rejected because:', self.diskItemFilter( item, requiredAttributes, explainRejection=True )
      if self._formatsWithConverter:
        requiredAttributes[ '_format' ] = oldFormats
    else:
      for i in readValues:
        yield i
  
  
  def diskItemFilter( self, *args, **kwargs ):
    return diskItemFilter( self.database, *args, **kwargs )
    #if diskItem.type is not None:
      #types = self.database.getTypeChildren( *self.database.getAttributeValues( '_type', {}, required ) )
      #if types and diskItem.type.name not in types:
        #if explainRejection:
          #return 'DiskItem type ' + repr(diskItem.type.name) + ' is not in ' + repr( tuple(types) )
        #return False
    #formats = self.database.getAttributeValues( '_format', {}, required )
    #if formats and not(diskItem.format is None) :
      #if diskItem.format.name not in formats:
        #if explainRejection:
          #if diskItem.format is None :
            #value = None
          #else :
            #value = diskItem.format.name
          #return 'DiskItem format ' + repr(value) + ' is not in ' + repr( tuple(formats) )
        #return False
    #for key in required:
      #if key in ( '_type', '_format' ): continue
      #values = self.database.getAttributeValues( key, {}, required )
      #itemValue = diskItem.get( key, Undefined )
      #if itemValue is Undefined or itemValue not in values:
        #if explainRejection:
          #if itemValue is Undefined:
            #return 'DiskItem do not have the required ' + repr( key ) + ' attribute'
          #else:
            #return 'DiskItem attribute ' + repr(key) + ' = ' + repr( itemValue ) + ' is not in ' + repr( tuple(values) )
        #return False
    #return True
  
  
  def typeInfo( self, translator = None ):
    if translator: translate = translator.translate
    else: translate = _
    formats = ''
    for f in self.formats:
      if formats: formats += ', '
      formats += translate( f.name )
    return  ( ( translate( 'Type' ), translate( self.type.name ) ),
              ( translate( 'Access' ), translate( 'input' ) ), 
              ( translate( 'Formats' ), formats ) )
  
  def toolTipText( self, parameterName, documentation ):
    result = '<center>' + parameterName
    if not self.mandatory: result += ' (' + _t_( 'optional' ) + ')'
    result += '</center><hr><b>' + _t_( 'Type' ) + \
              ':</b> ' + self.type.name + '<br><b>' + _t_( 'Formats' ) + \
              ':</b><blockquote>'
    br = 0
    for f in self.formats:
      if br:
        result += '<br>'
      else:
        br = 1
      result += f.name + ': ' + str(f.getPatterns().patterns[0].pattern)
    result += '</blockquote><b>' + _t_( 'Description' ) + '</b>:<br>' + \
              documentation
    return result


  def editor( self, parent, name, context ):
    return DiskItemEditor( self, parent, name, context=context )
  
  
  def editor( self, parent, name, context ):
    return DiskItemEditor( self, parent, name, context=context, write=self._write )


  def listEditor( self, parent, name, context ):
    return DiskItemListEditor( self, parent, name, context=context, write=self._write )
