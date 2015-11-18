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
from soma.spm.spm8.spatial.coregister.estimation_options import EstimationOptions
from soma.spm.spm8.spatial.coregister import Estimate
from soma.spm.spm_launcher import SPM8, SPM8Standalone
#------------------------------------------------------------------------------
configuration = Application().configuration
#------------------------------------------------------------------------------
def validation():
  try:
    SPM8Standalone(configuration)
  except:
    SPM8(configuration)
#------------------------------------------------------------------------------

configuration = Application().configuration

userLevel = 1
name = 'spm8 - Coregister: Estimate Only - generic'

estimation_section = "Estimation options"
reslice_section = "reslice options"

signature = Signature(
  "reference", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image']),
  "source", ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image']),
  "others", ListOf(ReadDiskItem("4D Volume", ['NIFTI-1 image', 'SPM image', 'MINC image'])),
  "objective_function", Choice("Mutual Information",
                               "Normalised Mutual Information",
                               "Entropy Correlation Coefficient",
                               "Normalised Cross Correlation"),
  "separation", ListOf(Float()),
  "tolerances", ListOf(Float()),
  "histogram_smoothing", ListOf(Float()),
  "interpolation", Choice("Nearest neighbour",
                          "Trilinear",
                          "2nd Degree B-Spline",
                          "3rd Degree B-Spline",
                          "4th Degree B-Spline",
                          "5th Degree B-Spline",
                          "6th Degree B-Spline",
                          "7th Degree B-Spline"),
  
  "extract_coregister_matrix", Boolean(),
  "coregister_matrix", WriteDiskItem("Transformation matrix", "Transformation matrix"),

  'batch_location', WriteDiskItem( 'Matlab SPM script', 'Matlab script', section='default SPM outputs' ),
)
def initialization(self):
  self.setOptional("others")
  
  self.addLink(None, "extract_coregister_matrix", self.updateSignatureAboutCoregisterMatrix)
  
  self.addLink("batch_location", "source", self.updateBatchPath)
    
  self.objective_function = "Normalised Mutual Information"
  self.separation = [4,2]
  self.tolerances = [0.02,  0.02,  0.02, 0.001, 0.001, 0.001,  0.01,  0.01,  0.01, 0.001, 0.001, 0.001]
  self.histogram_smoothing = [7,7]
  self.interpolation = "Trilinear"
  self.extract_coregister_matrix = False
   
def updateSignatureAboutCoregisterMatrix(self, proc):
  if self.extract_coregister_matrix:
    self.setEnable("coregister_matrix")
  else:
    self.setDisable("coregister_matrix")
  self.signatureChangeNotifier.notify( self )
    
def updateBatchPath(self, proc):
  if self.source is not None:
    directory_path = os.path.dirname(self.source.fullPath())
    return os.path.join(directory_path, 'spm8_coregister_estimate_job.m')
  
def execution( self, context ):
  
  estimation_options = EstimationOptions()
  if self.objective_function == "Mutual Information":
    estimation_options.setObjectiveFunctionToMutualInformation()
  elif self.objective_function == "Normalised Mutual Information":
    estimation_options.setObjectiveFunctionToNormalisedMutualInformation()
  elif self.objective_function == "Entropy Correlation Coefficient":
    estimation_options.setObjectiveFunctionToEntropyCorrelationCoefficient()
  elif self.objective_function == "Normalised Cross Correlation":
    estimation_options.setObjectiveFunctionToNormalisedCrossCorrelation()
  else:
    raise ValueError("Unvalid objective_function")

  estimation_options.setSeparation(self.separation)
  estimation_options.setTolerances(self.tolerances)
  estimation_options.setHistogramSmoothing(self.histogram_smoothing)

  estimate = Estimate()
  estimate.setReferenceVolumePath(self.reference.fullPath())
  estimate.setSourceVolumePath(self.source.fullPath())
  if self.others:
    for other_diskitem in self.others:
      estimate.addOtherVolumePath(other_diskitem.fullPath())

  estimate.replaceEstimationOptions(estimation_options)
  estimate.start(configuration, self.batch_location.fullPath())
  
  if self.extract_coregister_matrix and self.coregister_matrix is not None:
    #very usefull because spm action doesn't recompute minf, so the transformation in minf files is wrong
    self.source.clearMinf()
    self.extractCoregisterMatrix( self.source.fullPath(), self.reference.fullPath(), self.coregister_matrix.fullPath() )
    
def extractCoregisterMatrix(self, source_path, reference_path, output_path):

  source_vol = aims.read( source_path )
  source_aligned_trm = aims.AffineTransformation3d(source_vol.header()['transformations'][1])
  reference_vol = aims.read( reference_path )
  #If reference volume has two transformations (scanner + aligned)
  #the "aligned" trm of source "go" to the "aligned" of reference
  #else go to the "scanner" of reference
  if len(reference_vol.header()['transformations']) > 1:
    reference_aligned_trm = aims.AffineTransformation3d(reference_vol.header()['transformations'][1])
    reference_trm = reference_aligned_trm.inverse()
  else:
    reference_scanner_trm = aims.AffineTransformation3d(reference_vol.header()['transformations'][0])
    reference_trm = reference_scanner_trm.inverse()

  aims.write( reference_trm * source_aligned_trm, output_path )
  