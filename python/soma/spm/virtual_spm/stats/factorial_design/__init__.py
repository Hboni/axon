# -*- coding: utf-8 -*-
from soma.spm.spm_batch_maker_utils import addBatchKeyWordInEachItem

class FactorialDesign():
  """
  This  interface  is used for setting up analyses of PET data. It is also used for '2nd
  level' or 'random effects' analysis which allow one to make a population inference.
  First  level  models  can be used to produce appropriate summary data, which can
  then  be  used  as  raw  data  for  a  second-level  analysis. For example, a simple
  t-test  on  contrast  images from the first-level turns out to be a random-effects
  analysis  with  random  subject  effects,  inferring  for  the population based on a
  particular sample of subjects.
  """
#==============================================================================
#     Directory
#==============================================================================
  def setDirectory( self, directory_path ):
    """
    Select  a  directory  where the SPM.mat file containing the specified design matrix
    will be written.
    """
    self.directory.setDirectory( directory_path )
#==============================================================================
#     Covariate
#==============================================================================
  def appendCovariate(self, covariate):
    self.covariate_container.append(covariate)

  def clearCovariateList(self):
    self.covariate_container.clear()
#==============================================================================
#     Masking
#==============================================================================
  def enableImplicitMask(self):
    self.masking.enableImplicitMask()

  def disableImplicitMask(self):
    self.masking.disableImplicitMask()

  def setExplicitMask(self, explicit_mask_path):
    self.masking.setExplicitMask(explicit_mask_path)

  def removeExplicitMask(self):
    self.masking.removeExplicitMask()

  def setAbsoluteThresholdMethod(self):
    self.masking.setAbsoluteThresholdMethod()

  def setRelativeThresholdMethod(self):
    self.masking.setRelativeThresholdMethod()

  def unsetThreshold(self):
    self.masking.unsetThreshold()

  def setThreshold(self, threshold):
    self.masking.setThreshold(threshold)
#==============================================================================
#     global_calculation
#==============================================================================
  def setGlobalCalculationMethod(self, method):
    self.global_calculation.setMethod(method)

  def setValuesForUserGlobalCalculationMethod(self, values_list):
    self.global_calculation.setValuesForUserMethod(values_list)
#==============================================================================
# global_normalisation
#==============================================================================
  def enableOverallGrandMeanScaling(self):
    self.global_normalisation.enableOverallGrandMeanScaling()

  def setOverallGrandMeanScalingValue(self, grand_mean_scaled_value):
    self.global_normalisation.setOverallGrandMeanScalingValue(grand_mean_scaled_value)

  def disableOverallGrandMeanScaling(self):
    self.global_normalisation.disableOverallGrandMeanScaling()

  def unsetGlobalNormalisation( self):
    self.global_normalisation.unsetNormalisation()

  def setProportionalGlobalNormalisation(self):
    self.global_normalisation.setProportionalNormalisation()

  def setANCOVAGlobalNormalisation(self):
    self.global_normalisation.setANCOVANormalisation()
#==============================================================================
#
#==============================================================================
    
  def getStringListForBatch( self ):
    if self.design is not None:
      batch_list = []
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.directory.getStringListForBatch()))
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.design.getStringListForBatch()))
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.covariate_container.getStringListForBatch()))
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.masking.getStringListForBatch()))
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.global_calculation.getStringListForBatch()))
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.factorial_design", self.global_normalisation.getStringListForBatch()))
      return batch_list
    else:
      raise NotImplementedError("This is an abstract class, use its subclasses")
#==============================================================================
#==============================================================================
# # One Sample T Test
#==============================================================================
#==============================================================================
class OneSampleTTest():
  def setScans(self, scans):
    self.design.setScans(scans)
#==============================================================================
#==============================================================================
# # Two Sample T Test
#==============================================================================
#==============================================================================
class TwoSampleTTest():
  def setGroup1Scans(self, scans):
    self.design.setGroup1Scans(scans)

  def setGroup2Scans(self, scans):
    self.design.setGroup2Scans(scans)

  def enableIndependence(self):
    self.design.enableIndependence()

  def disableIndependence(self):
    self.design.disableIndependence()

  def setEqualVariance(self):
    self.design.setEqualVariance()

  def setUnequalVariance(self):
    self.design.setUnequalVariance()

  def enableGrandMeanScaling(self):
    self.design.enableGrandMeanScaling()

  def disableGrandMeanScaling(self):
    self.design.disableGrandMeanScaling()

  def enableANCOVA(self):
    self.design.enableANCOVA()

  def disableANCOVA(self):
    self.design.disableANCOVA()

#==============================================================================
#==============================================================================
# # Paired Sample T Test
#==============================================================================
#==============================================================================
class PairedTTest():
  def addScansPair(self, scans_list):
    self.design.addScansPair(scans_list)

  def clearScanPair(self):
    self.design.clearScanPair()

  def enableGrandMeanScaling(self):
    self.design.enableGrandMeanScaling()

  def disableGrandMeanScaling(self):
    self.design.disableGrandMeanScaling()

  def enableANCOVA(self):
    self.design.enableANCOVA()

  def disableANCOVA(self):
    self.design.disableANCOVA()
