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
from soma.minf.api import createReducerAndExpander, createMinfWriter, \
                          registerClass, writeMinf
from soma.uuid import Uuid
from soma.translation import translate as _
from soma.undefined import Undefined
from soma.minf.api import readMinf
from gzip import open as gzipOpen
import neuroLog, neuroConfig, neuroHierarchy, neuroException
from glob import glob
import weakref
import types

minfHistory = 'brainvisa-history_2.0'

class HistoryBook( object ):
  '''
  An L{HistoryBook} contains some L{HistoricalEvent}.
  '''
  
  _allBooks = weakref.WeakValueDictionary()
  
  def __new__( cls, directory, compression=False ):
    book = HistoryBook._allBooks.get( directory )
    if book is None:
      book = object.__new__( cls )
      HistoryBook._allBooks[ directory ] = book
    return book
  
  
  def __init__( self, directory, compression=False ):
    if hasattr( self, '_HistoryBook__dir' ):
      # self has already been created but __init__ is always
      # called after __new__
      return
    self.uuid = Uuid()
    if not os.path.isdir( directory ):
      os.makedirs( directory )
    self.__dir = directory
    self.__compression = compression
  
  def storeEvent( self, event, compression=None ):
    if isinstance( event, ProcessExecutionEvent ):
      # Store an event corresponding to current BrainVISA session
      # if it is not already done.
      bvsessionEvent = self.findEvent( event.content[ 'bvsession' ], None )
      if bvsessionEvent is None:
        bvsessionEvent = BrainVISASessionEvent()
        bvsessionEvent.setCurrentBrainVISASession()
        self.storeEvent( bvsessionEvent )
    if compression is None:
      compression = self.__compression
    #print '!history! store in', self.__dir, ':', event
    eventFileName = os.path.join( self.__dir,  str( event.uuid ) + '.' + event.eventType )
    event.save( eventFileName, compression ) 
  
  def findEvent( self, uuid, default=Undefined ):
    try:
      fileName = self._findEventFileName( uuid )
    except ValueError:
      if default is Undefined: raise
      return default
    return readMinf( fileName )[ 0 ]
    
  
  def _findEventFileName( self, uuid ):
    eventFilePattern = os.path.join( self.__dir,  str( uuid ) + '.*' )
    l = glob( eventFilePattern )
    if l:
      return l[0]
    raise ValueError( _( 'History book %(book)s does not contain event %(uuid)s' ) %
                        { 'book': unicode( self ), 'uuid': str(uuid) } )
  
  
  def removeEvent( self, uuid ):
    os.remove( self._findEventFileName( uuid ) )

  @staticmethod
  def getHistoryBookDirectories( item ):
    historyBook = None
    if hasattr( neuroConfig, 'historyBookDirectory' ):
      historyBook = neuroConfig.historyBookDirectory
    if not historyBook and item is not None:
      database = item.getHierarchy( '_database' )
      if database:
        db=neuroHierarchy.databases.database(database)
        if db is not None and db.activate_history:
          historyBook = os.path.join( database, 'history_book' )
    if type( historyBook ) in types.StringTypes:
      historyBook = [ historyBook ]
    return historyBook

  @staticmethod
  def storeProcessStart( executionContext, process ):
    # print '!history! storeProcessStart:', process
    historyBooksContext = {}
    for parameterized, attribute, type in process.getAllParameters():
      if isinstance( type, neuroHierarchy.WriteDiskItem ):
        item = getattr( parameterized, attribute )
        historyBooks = HistoryBook.getHistoryBookDirectories( item )
        if historyBooks:
          for historyBook in historyBooks:
            if not os.path.exists( historyBook ):
              os.mkdir(historyBook)
            historyBook = HistoryBook( historyBook, compression=True )
            print item
            historyBooksContext.setdefault( historyBook, {} )[ item.fullPath() ] = ( item, item.modificationHash() )

    event = None
    if historyBooksContext:
      # print '!history! databases:', [i._HistoryBook__dir for i in historyBooksContext.iterkeys()]
      event = executionContext.createProcessExecutionEvent()
      for book in historyBooksContext.iterkeys():
        book.storeEvent( event )
      event._logItem = executionContext._lastStartProcessLogItem
    return event, historyBooksContext


  @staticmethod
  def storeProcessFinished( executionContext, process, event, historyBooksContext ):
    #print '!history! storeProcessFinished:', process
    event.setLog( event._logItem )
    for book, items in historyBooksContext.iteritems():
      changedItems = [item for item, hash in items.itervalues() if hash != item.modificationHash()]
      event.content[ 'modified_data' ] = [unicode(item) for item in changedItems]
      book.storeEvent( event )
      for item in changedItems:
        try:
          item.setMinf( 'lastHistoricalEvent', event.uuid )
        except:
          neuroException.showException()
  
  

class HistoricalEvent( object ):
  """
  
  """
  def __init__( self, uuid = None ):
    if uuid is None: uuid = Uuid()
    self.uuid = uuid
  
  
  def save( self, eventFileName, compression=False ):
    close = True
    if type( eventFileName ) in ( str, unicode ):
      if compression:
        eventFile = gzipOpen( eventFileName, mode='w' )
      else:
        eventFile = open( eventFileName, mode='w' )
    else:
      eventFile = eventFileName
      close = False
    writeMinf( eventFile, ( self, ), reducer=minfHistory )
    if close:
      eventFile.close()


class ProcessExecutionEvent( HistoricalEvent ):
  """
  This object enables to store the state of a :py:class:`Process` instance in a dictionary format.
  """
  eventType = 'bvproc'
  
  def __init__( self, uuid=None, content={} ):
    HistoricalEvent.__init__( self, uuid )
    self.content = { 'bvsession': neuroConfig.sessionID }
    self.content.update( content )
    
  
  def __getinitargs__( self ):
    return ( self.uuid, self.content )
  
  
  def setProcess( self, process ):
    process.saveStateInDictionary( self.content )
  
  
  def setWindow( self, processView ):
    self.content[ 'window' ] = {
        'position': [ processView.x(), processView.y() ],
        'size': [ processView.width(), processView.height() ],
        #'state': processView.windowState(),
      }


  def setLog( self, log ):
    if log:
      if isinstance( log, neuroLog.LogFile.Item ):
        log._expand({})
        self.content[ 'log' ] = [ log ]
      else:
        self.content[ 'log' ] = list( neuroLog.expandedReader( log.fileName ) )


  def __str__( self ):
    if self.content.get('id', None):
      return 'bvproc<' + str(self.uuid) + ',' + self.content['id'] + '>'
    else:
      return str(self.content)

class BrainVISASessionEvent( HistoricalEvent ):
  eventType = 'bvsession'
  
  def __init__( self, uuid=None, content={} ):
    HistoricalEvent.__init__( self, uuid )
    self.content = content.copy()
  
  
  def __getinitargs__( self ):
    return ( self.uuid, self.content )
  
  
  def setCurrentBrainVISASession( self ):
    self.content[ 'version' ] = neuroConfig.versionString()
    self.uuid = neuroConfig.sessionID
    if neuroConfig.brainvisaSessionLogItem:
      neuroConfig.brainvisaSessionLogItem._expand({})
      self.content[ 'log' ] = [ neuroConfig.brainvisaSessionLogItem ]
  
  
  def __str__( self ):
    return 'bvsession<' + str(self.uuid) + '>'
