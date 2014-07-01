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

from brainvisa.processes import *
import brainvisa.tools.spm_run as spm
import brainvisa.tools.spm_utils as spmUtils

#------------------------------------------------------------------------------
configuration = Application().configuration
#------------------------------------------------------------------------------

def validation():
  return spm.validation(configuration)

#------------------------------------------------------------------------------

userLevel = 2
name = 'smooth one image (using SPM8)'

#------------------------------------------------------------------------------

signature = Signature(
    'img', ReadDiskItem('4D Volume', 'NIFTI-1 image'),
    'img_smoothed', WriteDiskItem('4D Volume', 'NIFTI-1 image'),
    'fwhm', String(),
    'dtype', String(),
    'im', String(),
    'prefix', String(),
    'batch_location', String(),

)

#------------------------------------------------------------------------------

def initialization(self):
    self.setOptional('batch_location')  
      
    spmUtils.initializeSmooth_withSPM8DefaultValues( self )

    self.signature['fwhm'].userLevel = 1
    self.signature['dtype'].userLevel = 1
    self.signature['prefix'].userLevel = 1
    self.signature['im'].userLevel = 1
    
    self.addLink( 'img_smoothed', 'img', self.update_img_smoothed )
    self.addLink( 'img_smoothed', 'prefix', self.update_img_smoothed )
    self.addLink( 'batch_location', 'img_smoothed', self.update_batch_location )
    
    
#
# Update output image when the input and a prefix are defined
#    
def update_img_smoothed( self, proc ):
    if self.img is None or self.prefix is None:
        return
    
    img_in_path = str(self.img)
    img_in_dir = img_in_path[:img_in_path.rindex('/')]
    img_in_fn = img_in_path[img_in_path.rindex('/') + 1:]
    
    return img_in_dir + "/" + self.prefix + "_" + img_in_fn 
    
    
#
# Update batch location in the same directory as 
# the image to smooth
#
def update_batch_location( self, proc ):
    
    if self.img_smoothed is not None:
        img_out_path = str( self.img_smoothed )
        img_out_dir = img_out_path[:img_out_path.rindex('/')+1]
        img_in_path = str( self.img ) 
        img_in_fn = img_in_path[img_in_path.rindex('/')+1:]
        img_in_ext = img_in_fn[img_in_fn.index('.'):]
        return img_out_dir + 'batch_smooth_' + img_in_path[img_in_path.rindex('/')+1:img_in_path.rindex('/')+1+len(img_in_fn)-len(img_in_ext)] + '.m'
    
    return ''
    

#------------------------------------------------------------------------------

def execution(self, context):  
    print "\n start ", name, "\n"
    
    if self.batch_location is not None:
        spmJobFile = self.batch_location
    else:
        outDir = self.img_smoothed.fullPath()[:self.img_smoothed.fullPath().rindex('/')]  
        spmJobFile = outDir + '/' + 'smooth_job.m'
    
    inDir = self.img.fullPath()[:self.img.fullPath().rindex('/')]  
    
    mat_file = open(spmJobFile, 'w')
    matfileDI = None
        
    matfilePath = spmUtils.writeSmoothMatFile(context, self.img.fullPath()
                                              , matfileDI, mat_file
                                              , self.fwhm, self.dtype, self.im, self.prefix)
     
    spm.run(context, configuration, matfilePath)    
    
    spmUtils.moveSpmOutFiles(inDir, self.img_smoothed.fullPath(), spmPrefixes=[ self.prefix.replace("""'""", '') + os.path.basename(self.img.fullPath()) ])
    
    print "\n stop ", name, "\n"

#------------------------------------------------------------------------------
