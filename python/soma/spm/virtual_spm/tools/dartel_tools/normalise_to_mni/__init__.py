# -*- coding: utf-8 -*-
from soma.spm.virtual_spm.tools.dartel_tools.normalise_to_mni.subject import Subject
from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsAllowed
from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsStrOrUnicode
from soma.spm.spm_batch_maker_utils import addBatchKeyWordInEachItem, convertlistToSPMString
from soma.spm.spm_batch_maker_utils import convertNumpyArrayToSPMString

import numbers
import numpy

class NormaliseToMNI():
  """
  Normally,  DARTEL  generates  warped images that align with the average-shaped
  template.  This  routine  includes  an  initial affine regisration of the template (the
  final one generated by DARTEL), with the TPM data released with SPM.
  "Smoothed  (blurred)  spatially  normalised  images  are generated in such a way
  that  the  original  signal  is  preserved.  Normalised  images  are generated by a
  "pushing" rather than a "pulling" (the usual) procedure. Note that a procedure
  related  to trilinear interpolation is used, and no masking is done.  It is therefore
  recommended   that   the  images  are  realigned  and  resliced  before  they  are
  spatially  normalised,  in  order  to  benefit  from motion correction using higher
  order  interpolation.  Alternatively, contrast images generated from unsmoothed
  native-space fMRI/PET data can be spatially normalised for a 2nd level analysis.
  Two  "preserve" options are provided.  One of them should do the equavalent of
  generating  smoothed "modulated" spatially normalised images.  The other does
  the  equivalent of smoothing the modulated normalised fMRI/PET, and dividing by
  the smoothed Jacobian determinants.
  """
  @checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
  def setFinalTemplatePath(self, final_template_path):
    """
    Select  the  final  Template file generated by DARTEL. This will be affine registered
    with  a  TPM  file,  such  that  the resulting spatially normalised images are closer
    aligned  to  MNI space. Leave empty if you do not wish to incorporate a transform
    to  MNI  space.
    """
    self.final_template_path = final_template_path

  def setAccordingToFewSubjectsContainer(self, subject):
    """
    Select  this  option  if  there are only a few subjects, each with many or a variable
    number  of scans each. You will then need to specify a series of subjects, and the
    flow field and images of each of them.
    """
    del self.according_to
    self.according_to = Subject()
    self.according_to.append(subject)

  def addSubject(self, subject):
    """
    Subject to be spatially normalized.
    """
    self.according_to.append(subject)

  def setAccordingToManySubjects(self, many_subjects):
    """
    Select  this  option if you have many subjects to spatially normalise, but there are
    a small and fixed number of scans for each subject.
    """
    del self.according_to
    self.according_to = many_subjects

  @checkIfArgumentTypeIsAllowed(list, 1)
  def setVoxelSize(self, voxel_size_list):
    """
    Specify the voxel sizes of the deformation field to be produced. Non-finite values
    will  default  to  the  voxel  sizes of the template imagethat was originally used to
    estimate the deformation.
    """
    if len(voxel_size_list) == 3:
      self.voxel_size = voxel_size_list
    else:
      raise ValueError("voxel_size_list must have 3 items [x, y, z]")

  @checkIfArgumentTypeIsAllowed(numpy.ndarray, 1)
  def setBoundingBox(self, numpy_array):
    """
    Specify  the  bounding  box  of  the  deformation field to be produced. Non-finite
    values  will  default  to the bounding box of the template imagethat was originally
    used to estimate the deformation.
    """
    if numpy_array.shape == (2, 3):
      self.bounding_box = numpy_array
    else:
      raise ValueError("An 2-by-3 array must be entered")

  def setPreserveToConcentrations(self):
    """
    Preserve  Concentrations:  Spatially  normalised  images  are not "modulated". The
    warped images preserve the intensities of the original images.
    """
    self.preserve = 0

  def setPreserveToAmount(self):
    """
    Preserve  Total:  Spatially  normalised images are "modulated" in order to preserve
    the  total amount of signal in the images. Areas that are expanded during warping
    are correspondingly reduced in intensity.
    """
    self.preserve = 1

  @checkIfArgumentTypeIsAllowed(numbers.Real, 1)
  @checkIfArgumentTypeIsAllowed(numbers.Real, 2)
  @checkIfArgumentTypeIsAllowed(numbers.Real, 3)
  def setFWHM(self, x, y ,z):
    """
    Specify the full-width at half maximum (FWHM) of the Gaussian blurring kernel in
    mm.  Three  values  should  be  entered,  denoting  the  FWHM  in  the  x, y and z
    directions.  Note  that you can also specify [0 0 0], but any ``modulated' data will
    show  aliasing  (see  eg  Wikipedia),  which occurs because of the way the warped
    images are generated.
    """
    self.fwhm = convertlistToSPMString([x, y, z])

  def getStringListForBatch( self ):
    if self.according_to is not None:
      batch_list = []
      batch_list.append("spm.tools.dartel.mni_norm.template = {'%s'};" % self.final_template_path)
      batch_list.extend(addBatchKeyWordInEachItem("spm.tools.dartel.mni_norm", self.according_to.getStringListForBatch()))
      batch_list.append("spm.tools.dartel.mni_norm.vox = %s;" %convertlistToSPMString(self.voxel_size))
      batch_list.append("spm.tools.dartel.mni_norm.bb = %s;" %convertNumpyArrayToSPMString(self.bounding_box))
      batch_list.append("spm.tools.dartel.mni_norm.preserve = %i;" %self.preserve)
      batch_list.append("spm.tools.dartel.mni_norm.fwhm = %s;" %self.fwhm)
      return batch_list
    else:
      raise ValueError('At least one according_to option is required')

  def _moveSPMDefaultPathsIfNeeded(self):
    pass





