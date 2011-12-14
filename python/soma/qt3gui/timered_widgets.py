# -*- coding: iso-8859-1 -*-

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

'''
L{QLineEditModificationTimer} and L{TimeredQLineEdit} classes associate a
L{QTimer<qt.QTimer>} to a L{QLineEdit<qt.QLineEdit>} in order to signal user
modification only after an inactivity period.

@author: Yann Cointepas
@organization: U{NeuroSpin<http://www.neurospin.org>} and U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

import qt

#-------------------------------------------------------------------------------
class QLineEditModificationTimer( qt.QObject ):
  '''
  A L{QLineEditModificationTimer} instance is accociated to a 
  L{QLineEdit<qt.QLineEdit>} instance, it listen all user modification (Qt 
  signal C{'textChanged( const QString & )'}) and emit a 
  C{PYSIGNAL('userModification' )} when C{timerInterval} milliseconds passed
  since the last user modification.
  '''
  
  #: Default timer interval in milliseconds
  defaultTimerInterval = 2000

  def __init__( self, qLineEdit, timerInterval=None ):
    '''
    @param qLineEdit: widget associated with this L{QLineEditModificationTimer}.
    @type  qLineEdit: L{QLineEdit<qt.QLineEdit>} instance
    @param timerInterval: minimum inactivity period before emitting
      C{userModification} signal. Default value is
      L{QLineEditModificationTimer.defaultTimerInterval}
    @type  timerInterval: milliseconds
    
    @see: L{TimeredQLineEdit}
    '''
    qt.QObject.__init__( self )
    #: L{QLineEdit<qt.QLineEdit>} instance associated with this 
    #: L{QLineEditModificationTimer}
    self.qLineEdit = qLineEdit
    if timerInterval is None:
      self.timerInterval = self.defaultTimerInterval
    else:
      #: minimum inactivity period before emitting C{userModification} signal.
      self.timerInterval = timerInterval
    self.__timer = qt.QTimer( self )
    self.__internalModification = False
    self.connect( self.qLineEdit, qt.SIGNAL( 'textChanged( const QString & )'), 
                  self._userModification )
    self.connect( self.qLineEdit, qt.SIGNAL( 'lostFocus()' ), 
                  self._noMoreUserModification,  )
    self.connect( self.__timer, qt.SIGNAL( 'timeout()' ), 
                  qt.PYSIGNAL( 'userModification' ) )


  def _userModification( self ):
    if not self.__internalModification:
      self.__timer.start( self.timerInterval, True )


  def _noMoreUserModification( self ):
    if self.__timer.isActive():
      self.__timer.stop()
      self.emit( qt.PYSIGNAL( 'userModification' ), () )


  def stopInternalModification( self ):
    '''
    Stop emitting C{userModification} signal when associated
    L{QLineEdit<qt.QLineEdit>} is modified.
    
    @see: L{startInternalModification}
    '''
    self.__internalModification = False


  def startInternalModification( self ):
    '''
    Restart emitting C{userModification} signal when associated
    L{QLineEdit<qt.QLineEdit>} is modified.
    
    @see: L{stopInternalModification}
    '''
    self.__internalModification = True


  def stop( self ):
    '''
    Stop the timer if it is active.
    '''
    self.__timer.stop()

  
  def isActive( self ):
    '''
    Returns True if the timer is active, or False otherwise.
    '''
    return self.__timer.isActive()


#-------------------------------------------------------------------------------
class TimeredQLineEdit( qt.QLineEdit ):
  '''
  Create a L{QLineEdit<qt.QLineEdit>} instance that has an private attribute
  containing a L{QLineEditModificationTimer} associated to C{self}. Whenever
  the internal L{QLineEditModificationTimer} emits a C{PYSIGNAL( 
  'userModification' )} signal, this signal is also emited by the 
  L{TimeredQLineEdit} instance.
  '''
  def __init__( self, *args, **kwargs ):
    '''
    All non keyword parameters of the constructor are passed to
    L{QLineEdit<qt.QLineEdit>} constructor. An optional C{timerInterval} 
    keyword parameter can be given, it is passed to 
    L{QLineEditModificationTimer} constructor. At the time this class was 
    created, L{QLineEdit<qt.QLineEdit>} constructor did not accept keyword 
    parameters.
    '''
    timerInterval = kwargs.pop( 'timerInterval', None )
    if kwargs:
      qt.QLineEdit.__init__( self, *args, **kwargs )
    else:
      qt.QLineEdit.__init__( self, *args )
    self.__timer = QLineEditModificationTimer( self,
                                               timerInterval=timerInterval )
    self.connect( self.__timer, qt.PYSIGNAL( 'userModification' ),
                  qt.PYSIGNAL( 'userModification' ) )


  def stopInternalModification( self ):
    '''
    @see: L{QLineEditModificationTimer.stopInternalModification}
    '''
    self.__timer.stopInternalModification()


  def startInternalModification( self ):
    '''
    @see: L{QLineEditModificationTimer.startInternalModification}
    '''
    self.__timer.startInternalModification()
