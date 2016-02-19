# -*- coding: utf-8 -*-
from soma.spm.spm_batch_maker_utils import convertPathListToSPMBatchString
from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsAllowed, checkIfArgumentTypeIsStrOrUnicode

class ManySubjects():
  """
  Select  this  option if you have many subjects to spatially normalise, but there are
  a small and fixed number of scans for each subject.
  """
  @checkIfArgumentTypeIsAllowed(list, 1)
  def setFlowFieldPathList(self, flow_field_path_list):
    """
    The  flow  fields  store  the deformation information. The same fields can be used
    for  both  forward  or  backward  deformations  (or  even, in principle, half way or
    exaggerated deformations).
    """
    self.flow_field_path_list = flow_field_path_list
    
  @checkIfArgumentTypeIsAllowed(list, 1)
  def appendImagePathList(self, image_path_list):
    """
    The  flow  field deformations can be applied to multiple images. At this point, you
    are choosing how many images each flow field should be applied to.
    """
    self.image_path_list_list.append(image_path_list)
    
  def getStringListForBatch( self ):
    if self.flow_field_path_list is not None and self.image_path_list_list:
      batch_list = []
      batch_list.append("data.subjs.flowfields = {%s};" % convertPathListToSPMBatchString(self.flow_field_path_list))
      images_batch_string = '{\n'
      for image_path_list in self.image_path_list_list:
        images_batch_string+=convertPathListToSPMBatchString(image_path_list)
      images_batch_string = '\n};'
      return batch_list
    else:
      raise ValueError("flow_field_path_list and at least image_path_list are required")
      