# Copyright CEA and IFR 49 (2000-2005)
#
#  This software and supporting documentation were developed by
#      CEA/DSV/SHFJ and IFR 49
#      4 place du General Leclerc
#      91401 Orsay cedex
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

from backwardCompatibleQt import *
import neuroConfig
from neuroData import *
from neuroHierarchy import *
from neuroProcesses import *
import neuroProcesses
import neuroProcessesGUI
import neuroLog
from brainvisa.mailing import *
import smtplib
import types
from soma.html import htmlEscape


#----------------------------------------------------------------------------
bugReportDialog = None

class BugReportDialog( QVBox ):
  def __init__( self, parent=None, name=None ):
    QVBox.__init__( self, parent, name )
    self.setSpacing( 2 )
    self.setMargin( 10 )
    self.setCaption( _t_( 'Bug report' ) )
    for field in ( 'From', 'To', 'Cc', 'Bcc' ):
      hb = QHBox( self )
      QLabel( _t_( field ), hb )
      setattr( self, 'led' + field, QLineEdit( hb ) )
    
    QLabel( _t_( 'Message' ), self )
    self.tedMessage = QTextEdit( self )

    self.chkSendLog = QCheckBox( _t_( 'Attach log file' ), self )
    self.chkSendLog.setChecked( 1 )
    
    self.ledFrom.setText( neuroConfig.userEmail )
    self.ledTo.setText( neuroConfig.supportEmail )
    
    hb = QHBox( self )
    hb.setSpacing(6)
    hb.setMargin(6)
    spacer = QSpacerItem(20,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
    hb.layout().addItem( spacer )
    btn = QPushButton( _t_( 'Send' ), hb )
    self.connect( btn, SIGNAL( 'clicked()' ), self._ok )
    btn = QPushButton( _t_( 'Cancel' ), hb )
    self.connect( btn, SIGNAL( 'clicked()' ), self._cancel )

  def _ok( self ):
    # Build email message
    outer = MIMEMultipart()
    outer['Subject'] = '[BrainVISA ' + neuroConfig.versionString() + '] Bug report'
    for field in ( 'From', 'Cc', 'Bcc' ):
      value = str( getattr( self, 'led'+ field ).text().latin1() )
      if value:
        outer[ field ] = value
    to = str( self.ledTo.text().latin1() )
    to = map( string.strip, string.split( to, ',' ) )
    outer[ 'To' ] = string.join( to, ', ' )
    
    outer.preamble = '\n'
    # To guarantee the message ends with a newline
    outer.epilogue = ''
    
    msg = MIMEText( str( self.tedMessage.text().latin1() ), 'plain' )
    outer.attach( msg )
    if self.chkSendLog.isChecked() and neuroConfig.logFileName:
      # Copy and expand log file
      tmp = defaultContext().temporary( 'File' )
      neuroLog.expandedCopy( neuroConfig.logFileName, tmp.fullPath() )

      # Attach log file content
      file = open( tmp.fullPath(), 'rb' )
      msg = MIMEBase( 'application', 'octet-stream' )
      msg.set_payload( file.read() )
      file.close()
      email.Encoders.encode_base64( msg )
      msg.add_header('Content-Disposition', 'attachment', filename=neuroConfig.logFileName )
      outer.attach( msg )

    # Send message
    s = smtplib.SMTP( neuroConfig.SMTP_server_name )
    content = outer.as_string()
    s.sendmail( outer[ 'From' ], to, content )
    s.close()      
    self.close( 0 )
    
  def _cancel( self ):
    self.close( 0 )


#----------------------------------------------------------------------------
def showBugReport():
  global bugReportDialog
  
  if bugReportDialog is None:
    bugReportDialog = BugReportDialog()
  bugReportDialog.hide()
  bugReportDialog.show()
  bugReportDialog.raiseW()
 


#----------------------------------------------------------------------------
def addSupportMenu( widget, menuBar ):
  if canMail:
    supportMenu = QPopupMenu( widget )
    menuBar.insertItem( _t_( "&Support" ), supportMenu )
    try:
      supportMenu.insertItem( _t_( 'Bug report' ), showBugReport )
    except:
      showException()
  else:
    defaultContext().log( _t_( 'Feature disabled' ),
                          html=_t_( 'Bug report feature is disabled due to ' \
                               'missing email module in python' ),
                          icon='warning.png' )

