# Copyright IFR 49 (1995-2009)
#
#  This software and supporting documentation were developed by
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

signature = Signature('number', Number())

def buildNewSignature( self, number ):
  # Create a new Signature object
  paramSignature = ['number', Number()]
  for i in xrange( number ):
	  paramSignature += [ 'n' + str( i ), Number() ]
  signature = Signature( *paramSignature )
  # Set optional parameters
  for i in xrange( number ):
	  signature[ 'n' + str( i ) ].mandatory = False

  # Change the signature
  self.changeSignature( signature )


def initialization( self ):
  self.number = 0
  # Call self.buildNewSignature each time number is changed
  self.addLink( None, 'number', self.buildNewSignature )


def execution( self, context ):
  # Print all parameters values
  for n in self.signature.keys():
	  context.write( n, '=', getattr( self, n ) )
  if self.number == 0:
    # Try dynamic signature in batch mode
	  context.runProcess("DynamicSignature",4,1.2,3.4,5.6,7.8)
  context.system( 'sleep', '2s' )
  raise Exception( 'Arrgh !' )
