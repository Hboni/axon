# -*- coding: utf-8 -*-
from soma.spm.spm_main_module import SPM12MainModule
from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsStrOrUnicode

from soma.spm.spm_batch_maker_utils import checkIfArgumentTypeIsAllowed
from soma.spm.spm_batch_maker_utils import moveFileAndCreateFoldersIfNeeded
from soma.spm.spm_batch_maker_utils import addBatchKeyWordInEachItem
from soma.spm.spm_batch_maker_utils import getTodayDateInSpmFormat
from soma.spm.spm12.stats.model_review.display import Display, DesignMatrix

import os
import shutil
#==============================================================================
#
#==============================================================================
class ModelReview(SPM12MainModule):
  def __init__(self):
    self.matlab_file_path = None
    self.display = DesignMatrix()
    self.print_result = "'ps'"

    self.output_results_path = None

  @checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
  def setMatlabFilePath(self, matlab_file_path):
    """
    Select the SPM.mat file that contains the design specification.
    """
    self.matlab_file_path = matlab_file_path

  @checkIfArgumentTypeIsAllowed(Display, 1)
  def setDisplay(self, display):
    """Select graphical report."""
    del self.display
    self.display = display

  def unsetPrintResults(self):
    self.print_result = 'false'

  def setPrintResultsToPS(self):
    self.print_result = "'ps'"

  def setPrintResultsToEPS(self):
    self.print_result = "'eps'"

  def setPrintResultsToPDF(self):
    self.print_result = "'pdf'"

  def setPrintResultsToJPEG(self):
    self.print_result = "'jpg'"

  def setPrintResultsToPNG(self):
    self.print_result = "'png'"

  def setPrintResultsToTIFF(self):
    self.print_result = "'tif'"

  def setPrintResultsToMatlabFigure(self):
    self.print_result = "'fig'"

  def setOutputResultPath(self, output_results_path):
    self.output_results_path = output_results_path

  def getStringListForBatch( self ):
    if self.matlab_file_path is not None:
      batch_list = []
      batch_list.append("spm.stats.review.spmmat = {'%s'};" % self.matlab_file_path)
      batch_list.extend(addBatchKeyWordInEachItem("spm.stats.review", self.display.getStringListForBatch()))
      batch_list.append("spm.stats.review.print = %s;" % self.print_result)
      return batch_list
    else:
      raise ValueError('Unvalid Model estimation, Mat file not found')

  def _moveSPMDefaultPathsIfNeeded(self):
    if self.output_results_path is not None and self.print_result != 'false':
      spm_date = getTodayDateInSpmFormat()
      workspace_diretory = os.path.dirname(self.matlab_file_path)
      ext = self.print_result.replace("'", '')
      spm_result_path = os.path.join(workspace_diretory, "spm_%s.%s" % (spm_date, ext))
      if os.path.exists(spm_result_path):
        moveFileAndCreateFoldersIfNeeded(spm_result_path,
                                         self.output_results_path)
      elif os.path.exists(os.path.join('/tmp', "spm_%s.%s" % (spm_date, ext))):
        moveFileAndCreateFoldersIfNeeded(os.path.join('/tmp', "spm_%s.%s" % (spm_date, ext)),
                                         self.output_results_path,
                                         no_image=True)
      else:
        raise RuntimeError("Output file not found")
    else:
      pass#default prefix used




