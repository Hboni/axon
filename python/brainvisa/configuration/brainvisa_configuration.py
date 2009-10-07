# -*- coding: iso-8859-1 -*-

#  This software and supporting documentation are distributed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
# This software is governed by the CeCIL license version 2 under
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
# knowledge of the CeCILL version 2 license and that you accept its terms.


'''
@author: Yann Cointepas
@organization: U{NeuroSpin<http://www.neurospin.org>} and U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"


from soma.wip.configuration import ConfigurationGroup
from soma.wip.temporary import getSystemDefaultTempDir
from soma.signature.api import HasSignature, Signature, VariableSignature, Unicode, \
                               Choice, OpenedChoice, Boolean, Sequence, FileName
from distutils.spawn import find_executable

#------------------------------------------------------------------------------
def htmlBrowsers():
  return [i for i in ( 'firefox', 'konqueror', 'explorer', 'opera',
                       '/Applications/Internet\\ Explorer.app/Contents' \
                       '/MacOS/Internet\\ Explorer',
                       '/Applications/Safari.app/Contents/MacOS/Safari', 
                       'mozilla', 'netscape' ) if find_executable( i ) ]


#------------------------------------------------------------------------------
class BrainVISAConfiguration( ConfigurationGroup ):
  
  label = 'BrainVISA'
  icon = 'brainvisa_small.png'
  
  class SupportConfiguration( HasSignature ):
    signature = Signature(
      'userEmail', Unicode, dict( defaultValue='', doc='Your email address that will appear in the "From:" field of messages send to BrainVISA support team.' ),
      'supportEmail', Unicode, dict( defaultValue='support@brainvisa.info', doc='Email address of BrainVISA support team.' ),
      'smtpServer', Unicode, dict( defaultValue='', doc='Address of the server that wil be used to send emails.' ),
    )
  
  class SPMConfiguration( HasSignature ):
    signature = Signature(
      'SPM99_compatibility', Boolean, dict( defaultValue=False, doc='If selected, Analyse (*.hdr + *.img) images loaded from BrainVISA, Anatomist and Aims will use an heuristic to guess the orientationin in SPM99 compatible way.' ),
      'radiological_orientation', Boolean, dict( defaultValue=True, doc='If selected, SPM is supposed to use radiological orientation for images. Otherwise it is supposed to use neurological convention.' ),
    )

  signature = Signature(
    'userLevel', Choice( ( 'Basic', 0 ), 
                         ( 'Advanced', 1 ),
                         ( 'Expert', 2 ) ),
                 dict( defaultValue=0, doc='User level is used to hide experimental processes (and in some rare cases hide experimental parameters).' ),
    'language', Choice( ( 'default', None ), ( 'english', 'en' ), ( 'french', 'fr' ) ),
                dict( defaultValue=None, doc='Language of the graphical interface (it is necessary to restart BrainVISA to take modification into account).' ),
    'processesPath', Sequence( FileName( directoryOnly=True ) ), dict( defaultValue=[], doc='List of directories containing BrainVISA processes.' ),
    'temporaryDirectory', FileName( directoryOnly=True ), dict( defaultValue=getSystemDefaultTempDir(), doc='Directory where temporary files are stored. Name of temporary files produced by BrainVISA starts with <tt>"bv_"</tt>.' ),
    'textEditor', FileName, dict( defaultValue='', doc='Location of the program used to edit text files.' ),
    'htmlBrowser', OpenedChoice( ( '<built-in>', '' ), *htmlBrowsers() ), dict( defaultValue='', doc='Location of the program used to display HTML files.' ),
    'removeTemporary', Boolean, dict( defaultValue=True, doc='unselect this option if you do not want temporary files and directories to be automatically deleted. This option is used for debugging. If unselected BrainVISA can leave a lot of files in temporary directory.' ),
    'SPM', SPMConfiguration, dict( defaultValue=SPMConfiguration() ),
    'support', SupportConfiguration, dict( defaultValue=SupportConfiguration() ),
    'gui_style', OpenedChoice( ('<system default>', None ) ), dict( defaultValue = None, doc='Style of the graphical interface.' ),
    'databasesWarning', Boolean, dict( defaultValue=True, doc='Unselect this option to disable the warning message that is shown when you have not created any database.'),
  )


  def _check_userLevel_value( self, value ):
    return int( value )
  
  
  def __init__( self ):
    super( BrainVISAConfiguration, self ).__init__()
    self.signature = VariableSignature( self.signature )
