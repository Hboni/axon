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
from soma.spm.spm8.spatial.normalise import EstimateAndWrite
from soma.spm.spm8.spatial.normalise.subject import SubjectToEstimateAndWrite
from soma.spm.spm8.spatial.normalise.writing_options import WritingOptions
from soma.spm.spm8.spatial.normalise.estimation_options import EstimationOptions
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
  return spm
#------------------------------------------------------------------------------

userLevel = 1
name = 'spm8 - Normalise: Estimate & Write - generic'

subject_section = "subject options"
estimation_section = "Estimation options"
writing_section = "writing options"

signature = Signature(
  "source", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image'], section=subject_section),
  "source_weighting", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image'], section=subject_section),
  "images_to_write", ListOf(ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image']), section=subject_section),
  
  "template", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image'], section=estimation_section),
  "template_weighting", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image'], section=estimation_section),
  "source_smoothing", Float(section=estimation_section),
  "template_smoothing", Float(section=estimation_section),
  "affine_regularisation", Choice("ICBM space template",
                                  "Average sized template",
                                  "No regularisation", section=estimation_section),
  "frequency_cutoff", Float(section=estimation_section),
  "iterations", Integer(section=estimation_section),
  "regularisation", Float(section=estimation_section),
                     
  "preserve", Choice("Preserve Concentrations",
                     "Preserve Amount",
                     section=writing_section),
  "bounding_box", Matrix(length=2, width=3, section=writing_section),
  "voxel_size", ListOf(Float(),section=writing_section),           
  "interpolation", Choice("Nearest neighbour",
                          "Trilinear",
                          "2nd Degree B-Spline",
                          "3rd Degree B-Spline",
                          "4th Degree B-Spline",
                          "5th Degree B-Spline",
                          "6th Degree B-Spline",
                          "7th Degree B-Spline",
                          section=writing_section),
  "wrappping", Choice(("No wrap",[False, False, False]),
                      ("Wrap X",[True, False, False]),
                      ("Wrap Y",[False, True, False]),
                      ("Wrap X & Y",[True, True, False]),
                      ("Wrap Z",[False, False, True]),
                      ("Wrap X & Z",[True, False, True]),
                      ("Wrap Y & Z",[False, True, True]),
                      ("Wrap X, Y & Z",[True, True, True]),
                      section=writing_section),

  "custom_outputs", Boolean(section='outputs'),
  "filename_prefix", String(section='outputs'),
  "images_written", ListOf(WriteDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image']), section="outputs"),
  'batch_location', WriteDiskItem( 'Matlab SPM script', 'Matlab script', section='default SPM outputs' ),
)
def initialization(self):
  self.setOptional("source_weighting", "template_weighting")
  self.addLink(None, "custom_outputs", self.updateSignatureAboutOutputs)
  self.addLink(None, "filename_prefix", self.checkIfNotEmpty)
  
  self.addLink("batch_location", "source", self.updateBatchPath)
  
  #SPM default initialisation
  self.source_smoothing = 8
  self.template_smoothing = 0
  self.affine_regularisation = "ICBM space template"
  self.frequency_cutoff = 25
  self.iterations = 16
  self.regularisation = 1
  self.preserve = "Preserve Concentrations"
  self.bounding_box = [[-78, -112, -50],[78, 76, 85]]
  self.voxel_size = [2, 2, 2]
  self.interpolation = "Trilinear"
  self.wrappping = "No wrap"
  self.filename_prefix = 'w'
  
def updateSignatureAboutOutputs(self, proc):
  if self.custom_outputs:
    self.setEnable("images_written")
    self.setDisable("filename_prefix")
  else:
    self.setDisable("images_written")
    self.setEnable("filename_prefix")
  self.changeSignature(self.signature)
  
def checkIfNotEmpty(self, proc):
  if self.filename_prefix in [None, '']:
    self.filename_prefix = 'w'
  else:
    pass
    
def updateBatchPath(self, proc):
  if self.source is not None:
    directory_path = os.path.dirname(self.source.fullPath())
    return os.path.join(directory_path, 'spm8_normalise_EW_job.m')
  
def execution( self, context ):
  estimate_and_write = EstimateAndWrite()
  
  subject = SubjectToEstimateAndWrite()
  subject.setSourceImage(self.source.fullPath())
  if self.source_weighting is not None:
    subject.setSourceWeightingImage(self.source_weighting.fullPath())
  subject.setImageListToWrite([diskitem.fullPath() for diskitem in self.images_to_write])
  if self.custom_outputs and self.images_written:
    if len(self.images_to_write) == len(self.images_written):
      subject.setImageListWritten([diskitem.fullPath() for diskitem in self.images_written])
    else:
      raise ValueError("images_to_write and images_written must have the same length")
  else:
    pass#SPM default outputs
  
  estimate_and_write.appendSubject(subject)
  
  estimate = EstimationOptions()
  estimate.setTemplateImage(self.template.fullPath())
  if self.template_weighting is not None:
    estimate.setTemplateWeightingImage(self.template_weighting.fullPath())
  estimate.setSourceImageSmoothing(self.source_smoothing)
  estimate.setTemplateImageSmoothing(self.template_smoothing)
  if self.affine_regularisation == "ICBM space template":
    estimate.setAffineRegularisationToICBMSpaceTemplate()
  elif self.affine_regularisation == "Average sized template":
    estimate.setAffineRegularisationToAverageSizedTemplate()
  elif self.affine_regularisation == "No regularisation":
    estimate.unsetAffineRegularisation()
  else:
    raise ValueError("Unvalid choice for affine_regularisation")
  
  estimate.setNonLinearFrequencyCutOff(self.frequency_cutoff)
  estimate.setNonLinearIterations(self.iterations)
  estimate.setNonLinearRegularisation(self.regularisation)
  
  estimate_and_write.replaceEstimateOptions(estimate)
  
  writing = WritingOptions()
  if self.preserve == "Preserve Concentrations":
    writing.setPreserveToConcentrations()
  elif self.preserve == "Preserve Amount":
    writing.setPreserveToAmount()
  else:
    raise ValueError("Unvalid choice for preserve")
  
  writing.setBoundingBox(numpy.array(self.bounding_box))
  writing.setVoxelSize(self.voxel_size)
  
  if self.interpolation == "Nearest neighbour":
    writing.setInterpolationToNearestNeighbour()
  elif self.interpolation == "Trilinear":
    writing.setInterpolationToTrilinear()
  elif self.interpolation == "2nd Degree B-Spline":
    writing.setInterpolationTo2ndDegreeBSpline()
  elif self.interpolation == "3rd Degree B-Spline":
    writing.setInterpolationTo3rdDegreeBSpline()
  elif self.interpolation == "4th Degree B-Spline":
    writing.setInterpolationTo4thDegreeBSpline()
  elif self.interpolation == "5th Degree B-Spline":
    writing.setInterpolationTo5thDegreeBSpline()
  elif self.interpolation == "6th Degree B-Spline":
    writing.setInterpolationTo6thDegreeBSpline()
  elif self.interpolation == "7th Degree B-Spline":
    writing.setInterpolationTo7thDegreeBSpline()
  else:
    raise ValueError("Unvalid interpolation")
  
  writing.setWrapping(self.wrappping[0], self.wrappping[1], self.wrappping[2])
  writing.setFilenamePrefix(self.filename_prefix)
  
  estimate_and_write.replaceWrintingOptions(writing)
    
  spm = validation()
  spm.addModuleToExecutionQueue(estimate_and_write)
  spm.setSPMScriptPath(self.batch_location.fullPath())
  spm.run()
  