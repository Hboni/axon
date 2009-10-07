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

from neuroProcesses import *
import shfjGlobals

name = '2 - Primal Sketch from SPMt'

userLevel = 2

signature = Signature(
    'spmt', ReadDiskItem( 'SPMt map', 'Aims readable volume formats' ),
    'mask', ReadDiskItem( 'Functional mask', 'Aims readable volume formats' ),
    'sketch', WriteDiskItem( 'Primalsketch graph', 'Graph' ),
    'tMin', Float(),
    'tMax', Float(),
    'statFile', Choice('no stat file', 'SPMt : 61x73x61 voxels, 3x3x3 mm^3','SPMt : 91x109x91 voxels, 2x2x2 mm^3')
    )

def initialization( self ):
     self.linkParameters( 'mask', 'spmt' )
     self.linkParameters( 'sketch', 'spmt' )
     self.tMin = 1.0
     self.tMax = 64.0

def execution( self, context ):
     scales=context.temporary( 'GIS image' )
     blobs=context.temporary( 'GIS image' )

     call_list = [ 'AimsImagePrimalSketch',
                   '-i', self.spmt,
                   '-m', self.mask,
                   '-os', scales,
                   '-ob', blobs,
                   '-t1', self.tMin,
                   '-t2', self.tMax,
                   '-s', '0',
                   '-og', self.sketch,
                   '-sj', self.mask ]
     option_list = []
     if self.statFile=='SPMt : 61x73x61 voxels, 3x3x3 mm^3' :
          option_list += ['-f', '/home/olivier/Perforce/shared-personal/blobsStats/spmt_61-73-61_3-3-3/blobs.stat']
     elif self.statFile=='SPMt : 91x109x91 voxels, 2x2x2 mm^3' :
               option_list += ['-f', '/home/olivier/Perforce/shared-personal/blobsStats/spmt_91-109-91_2-2-2/blobs.stat']
     else :
          context.write( 'Starting with no stat file. Sure about that ?' )

     context.write('Starting primal sketch computation')
     apply( context.system, call_list+option_list )
     context.write('Finished')

