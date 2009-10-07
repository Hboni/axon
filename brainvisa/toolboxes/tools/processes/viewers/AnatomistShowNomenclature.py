#  This software and supporting documentation are distributed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
# This software is governed by the CeCILL-B license under
# French law and abiding by the rules of distribution of free software.
# You can  use, modify and/or redistribute the software under the 
# terms of the CeCILL-B license as circulated by CEA, CNRS
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
# knowledge of the CeCILL-B license and that you accept its terms.

from neuroProcesses import *
from brainvisa import anatomist

name = 'Anatomist Show Nomenclature'
roles = ('viewer',)
userLevel = 0

def validation():
    anatomist.validation()
  
signature = Signature(
  'read', ReadDiskItem( 'Nomenclature', [ 'Hierarchy' ] ),
  'use_existing_browser', Boolean(),
)


def initialization( self ):
  self.use_existing_browser = True


def execution( self, context ):
    a = anatomist.Anatomist()
    bro = None
    hie = a.loadObject( self.read )
    if self.use_existing_browser:
      wins = hie.getWindows()
      for w in wins:
        wi = w.getInfos()
        if wi.get( 'windowType' ) == 'Browser':
          bro = w
    if bro is None:
      bro = a.createWindow( 'Browser' )
      bro.addObjects( [hie] )
    return ( hie, bro )
