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
from soma.spm.spm8.tools.dartel_tools.normalise_to_mni import NormaliseToMNI
from soma.spm.spm8.tools.dartel_tools.normalise_to_mni.many_subjects import ManySubjects
from soma.spm.spm_launcher import SPM8, SPM8Standalone
import numpy
#------------------------------------------------------------------------------
configuration = Application().configuration
#------------------------------------------------------------------------------
def validation():
  try:
    spm = SPM8Standalone(configuration.SPM.spm8_standalone_command,
                         configuration.SPM.spm8_standalone_mcr_path,
                         configuration.SPM.spm8_standalone_path)
  except:
    spm = SPM8(configuration.SPM.spm8_path,
               configuration.matlab.executable,
               configuration.matlab.options)
#------------------------------------------------------------------------------

userLevel = 0
name = 'spm8 - DARTEL - Normalise to mni'

#------------------------------------------------------------------------------

signature = Signature(
  'template', ReadDiskItem( "TPM HDW DARTEL created template", "NIFTI-1 image" ),
  'flow_fields', ListOf( WriteDiskItem( "HDW DARTEL flow field", "NIFTI-1 image" ) ),
  'images_0', ListOf( ReadDiskItem( "T1 MRI tissue probability map", ["NIFTI-1 image", "SPM image", "MINC image"] ) ),
  
  "preserve", Choice("Preserve Concentrations",
                     "Preserve Amount"),
  "bounding_box", Matrix(length=2, width=3),
  "voxel_size", ListOf(Float()),  
  "fwhm", ListOf(Float()),
  
  'batch_location', WriteDiskItem( 'Matlab SPM script', 'Matlab script', section='default SPM outputs'),
 )

#------------------------------------------------------------------------------

def initialization(self):
  self.setOptional('template')

#------------------------------------------------------------------------------
def execution( self, context ):
  normalise = NormaliseToMNI()
  
  if self.final_template is not None:
    normalise.setFinalTemplatePath(self.final_template.fullPath())
    
  many_subjects = ManySubjects()
  many_subjects.setFlowFieldPathList([flow_field.fullPath() for flow_field in self.flow_fields])
  many_subjects.appendImagePathList([image.fullPath() for image in self.images_0])
  
  normalise.setAccordingToManySubjects(many_subjects)
  
  if self.preserve == "Preserve Concentrations":
    normalise.setPreserveToConcentrations()
  elif self.preserve == "Preserve Amount":
    normalise.setPreserveToAmount()
  else:
    raise ValueError("Unvalid preserve")

  normalise.setBoundingBox(numpy.array(self.bounding_box))
  normalise.setVoxelSize(self.voxel_size)

  if len(self.fwhm) == 3:
    normalise.setFWHM(self.fwhm[0], self.fwhm[1], self.fwhm[2])
  else:
    raise ValueError("Three  values  should  be  entered,  denoting the FWHM in the x, y and z  directions")
  
  spm = validation()
  spm.addModuleToExecutionQueue(normalise)
  spm.setSPMScriptPath(self.batch_location.fullPath())
  spm.run()
                     