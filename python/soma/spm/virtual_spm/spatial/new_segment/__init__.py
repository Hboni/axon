# -*- coding: utf-8 -*-
from soma.spm.virtual_spm.spatial.new_segment.channel import Channel
from soma.spm.virtual_spm.spatial.new_segment.tissue import Tissue

from soma.spm.spm_batch_maker_utils import addBatchKeyWordInEachItem, moveSPMPath, convertlistToSPMString
from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsAllowed, checkIfArgumentTypeIsStrOrUnicode

class NewSegment():
  """
  This  toolbox  is  currently only work in progress, and is an extension of the default unified
  segmentation.    The  algorithm  is  essentially  the  same  as  that  described in the Unified
  Segmentation paper, except for (i) a slightly different treatment of the mixing proportions,
  (ii)  the use of an improved registration model, (iii) the ability to use multi-spectral data, (iv)
  an  extended  set  of  tissue probability maps, which allows a different treatment of voxels
  outside  the  brain.  Some of the options in the toolbox do not yet work, and it has not yet
  been  seamlessly  integrated into the SPM8 software.  Also, the extended tissue probability
  maps need further refinement. The current versions were crudely generated (by JA) using
  data  that  was  kindly  provided  by  Cynthia  Jongen  of  the  Imaging Sciences Institute at
  Utrecht, NL.
  """    
  @checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
  @checkIfArgumentTypeIsStrOrUnicode(argument_index=2)
  def setSPMDefaultSetting(self, volume_path, tissue_proba_map_path):
    self._setSPMDefaultChannel(volume_path)
    self._setSPMDefautTissues(tissue_proba_map_path)
    
  def _setSPMDefaultChannel(self, volume_path):
    default_channel = Channel()
    default_channel.setVolumePath(volume_path)
    self.channel_container.setChannelList([default_channel])
    
  def _setSPMDefautTissues(self, tissue_proba_map_path):
    first_tissue = Tissue()
    first_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    first_tissue.setTissueProbilityDimension(1)
    first_tissue.setGaussianNumber(2)
    first_tissue.setNativeTissueNativeSpace()
    first_tissue.unsetWarpedTissue()
    self.tissue_container.append(first_tissue)
    
    second_tissue = Tissue()
    second_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    second_tissue.setTissueProbilityDimension(2)
    second_tissue.setGaussianNumber(2)
    second_tissue.setNativeTissueNativeSpace()
    second_tissue.unsetWarpedTissue()
    self.tissue_container.append(second_tissue)
    
    third_tissue = Tissue()
    third_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    third_tissue.setTissueProbilityDimension(3)
    third_tissue.setGaussianNumber(2)
    third_tissue.setNativeTissueNativeSpace()
    third_tissue.unsetWarpedTissue()
    self.tissue_container.append(third_tissue)
    
    fourth_tissue = Tissue()
    fourth_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    fourth_tissue.setTissueProbilityDimension(4)
    fourth_tissue.setGaussianNumber(3)
    fourth_tissue.setNativeTissueNativeSpace()
    fourth_tissue.unsetWarpedTissue()
    self.tissue_container.append(fourth_tissue)
    
    fifth_tissue = Tissue()
    fifth_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    fifth_tissue.setTissueProbilityDimension(5)
    fifth_tissue.setGaussianNumber(4)
    fifth_tissue.setNativeTissueNativeSpace()
    fifth_tissue.unsetWarpedTissue()
    self.tissue_container.append(fifth_tissue)
    
    sixth_tissue = Tissue()
    sixth_tissue.setTissueProbilityMapPath(tissue_proba_map_path)
    sixth_tissue.setTissueProbilityDimension(6)
    sixth_tissue.setGaussianNumber(2)
    sixth_tissue.unsetNativeTissue()
    sixth_tissue.unsetWarpedTissue()
    self.tissue_container.append(sixth_tissue)
    
    
  def appendChannel(self, channel):
    self.channel_container.append(channel)
    
  def appendTissue(self, tissue):
    self.tissue_container.append(tissue)
    
  @checkIfArgumentTypeIsAllowed(float, 1)
  def setMRFParameter(self, MRF_parameter):
    """
    When  tissue  class  images  are  written  out, a few iterations of a simple Markov
    Random  Field  (MRF)  cleanup  procedure  are  run.    This parameter controls the
    strength of the MRF. Setting the value to zero will disable the cleanup.
    Evaluated statements are entered.
    """
    self.MRF_parameter = MRF_parameter
    
  def unsetMRF(self):
    """
    When  tissue  class  images  are  written  out, a few iterations of a simple Markov
    Random  Field  (MRF)  cleanup  procedure  are  run.    This parameter controls the
    strength of the MRF. Setting the value to zero will disable the cleanup.
    Evaluated statements are entered.
    """
    self.MRF_parameter = 0
    
  @checkIfArgumentTypeIsAllowed(int, 1)
  def setWarpingRegularisation(self, regularisation_number):
    """
    The  objective function for registering the tissue probability maps to the image to
    process, involves minimising the sum of two terms. One term gives a function of
    how probable the data is given the warping parameters. The other is a function of
    how   probable   the   parameters   are,   and   provides   a   penalty   for  unlikely
    deformations.  Smoother  deformations  are  deemed  to  be  more  probable. The
    amount  of regularisation determines the tradeoff between the terms. Pick a value
    around one.  However, if your normalised images appear distorted, then it may be
    an idea to increase the amount of regularisation (by an order of magnitude). More
    regularisation  gives  smoother  deformations,  where the smoothness measure is
    determined by the bending energy of the deformations.
    """
    self.warping_regularisation = regularisation_number
    
  def setAffineRegularisationToEuropeanBrains(self):
    """
    The  procedure  is  a  local  optimisation,  so  it  needs  reasonable  initial starting
    estimates.  Images  should  be placed in approximate alignment using the Display
    function  of  SPM  before  beginning. A Mutual Information affine registration with
    the   tissue   probability   maps   (D'Agostino   et   al,  2004)  is  used  to  achieve
    approximate  alignment.  Note  that  this  step  does  not  include  any  model for
    intensity non-uniformity. This means that if the procedure is to be initialised with
    the  affine  registration,  then  the  data  should  not  be  too  corrupted with this
    artifact.If  there  is  a lot of intensity non-uniformity, then manually position your
    image  in  order  to  achieve  closer  starting  estimates,  and  turn  off  the affine
    registration.
    """
    self.affine_regularisation = 'mni'
    
  def setAffineRegularisationToAsianBrains(self):
    """
    The  procedure  is  a  local  optimisation,  so  it  needs  reasonable  initial starting
    estimates.  Images  should  be placed in approximate alignment using the Display
    function  of  SPM  before  beginning. A Mutual Information affine registration with
    the   tissue   probability   maps   (D'Agostino   et   al,  2004)  is  used  to  achieve
    approximate  alignment.  Note  that  this  step  does  not  include  any  model for
    intensity non-uniformity. This means that if the procedure is to be initialised with
    the  affine  registration,  then  the  data  should  not  be  too  corrupted with this
    artifact.If  there  is  a lot of intensity non-uniformity, then manually position your
    image  in  order  to  achieve  closer  starting  estimates,  and  turn  off  the affine
    registration.
    """
    self.affine_regularisation = 'eastern'
    
  def setAffineRegularisationToAverageSizedTemplate(self):
    """
    The  procedure  is  a  local  optimisation,  so  it  needs  reasonable  initial starting
    estimates.  Images  should  be placed in approximate alignment using the Display
    function  of  SPM  before  beginning. A Mutual Information affine registration with
    the   tissue   probability   maps   (D'Agostino   et   al,  2004)  is  used  to  achieve
    approximate  alignment.  Note  that  this  step  does  not  include  any  model for
    intensity non-uniformity. This means that if the procedure is to be initialised with
    the  affine  registration,  then  the  data  should  not  be  too  corrupted with this
    artifact.If  there  is  a lot of intensity non-uniformity, then manually position your
    image  in  order  to  achieve  closer  starting  estimates,  and  turn  off  the affine
    registration.
    """
    self.affine_regularisation = 'subj'
    
  def unsetAffineRegularisation(self):
    """
    The  procedure  is  a  local  optimisation,  so  it  needs  reasonable  initial starting
    estimates.  Images  should  be placed in approximate alignment using the Display
    function  of  SPM  before  beginning. A Mutual Information affine registration with
    the   tissue   probability   maps   (D'Agostino   et   al,  2004)  is  used  to  achieve
    approximate  alignment.  Note  that  this  step  does  not  include  any  model for
    intensity non-uniformity. This means that if the procedure is to be initialised with
    the  affine  registration,  then  the  data  should  not  be  too  corrupted with this
    artifact.If  there  is  a lot of intensity non-uniformity, then manually position your
    image  in  order  to  achieve  closer  starting  estimates,  and  turn  off  the affine
    registration.
    """
    self.affine_regularisation = ''
    
  def unsetRegularisation(self):
    """
    The  procedure  is  a  local  optimisation,  so  it  needs  reasonable  initial starting
    estimates.  Images  should  be placed in approximate alignment using the Display
    function  of  SPM  before  beginning. A Mutual Information affine registration with
    the   tissue   probability   maps   (D'Agostino   et   al,  2004)  is  used  to  achieve
    approximate  alignment.  Note  that  this  step  does  not  include  any  model for
    intensity non-uniformity. This means that if the procedure is to be initialised with
    the  affine  registration,  then  the  data  should  not  be  too  corrupted with this
    artifact.If  there  is  a lot of intensity non-uniformity, then manually position your
    image  in  order  to  achieve  closer  starting  estimates,  and  turn  off  the affine
    registration.
    """
    self.affine_regularisation = 'none'
     
  @checkIfArgumentTypeIsAllowed(float, 1)
  def setSamplingDistance(self, sampling_distance):
    """
    This  encodes the approximate distance between sampled points when estimating
    the  model  parameters. Smaller values use more of the data, but the procedure is
    slower  and  needs  more  memory.  Determining  the  ``best''  setting involves a
    compromise between speed and accuracy.
    """
    self.sampling_distance = sampling_distance
      
  def discardDeformationField(self):
    """
    Deformation  fields  can  be  saved  to disk, and used by the Deformations Utility.
    For   spatially   normalising  images  to  MNI  space,  you  will  need  the  forward
    deformation,  whereas for spatially normalising (eg) GIFTI surface files, you'll need
    the  inverse. It is also possible to transform data in MNI space on to the individual
    subject, which also requires the inverse transform. Deformations are saved as .nii
    files, which contain three volumes to encode the x, y and z coordinates.
    """
    self.deformation_fields = [0, 0]
  
  def saveDeformationFieldInverse(self): 
    """
    Deformation  fields  can  be  saved  to disk, and used by the Deformations Utility.
    For   spatially   normalising  images  to  MNI  space,  you  will  need  the  forward
    deformation,  whereas for spatially normalising (eg) GIFTI surface files, you'll need
    the  inverse. It is also possible to transform data in MNI space on to the individual
    subject, which also requires the inverse transform. Deformations are saved as .nii
    files, which contain three volumes to encode the x, y and z coordinates.
    """
    self.deformation_fields = [1, 0] 
  
  def saveDeformationFieldForward(self):   
    """
    Deformation  fields  can  be  saved  to disk, and used by the Deformations Utility.
    For   spatially   normalising  images  to  MNI  space,  you  will  need  the  forward
    deformation,  whereas for spatially normalising (eg) GIFTI surface files, you'll need
    the  inverse. It is also possible to transform data in MNI space on to the individual
    subject, which also requires the inverse transform. Deformations are saved as .nii
    files, which contain three volumes to encode the x, y and z coordinates.
    """
    self.deformation_fields = [0, 1] 
  
  def saveDeformationFieldInverseAndForward(self): 
    """
    Deformation  fields  can  be  saved  to disk, and used by the Deformations Utility.
    For   spatially   normalising  images  to  MNI  space,  you  will  need  the  forward
    deformation,  whereas for spatially normalising (eg) GIFTI surface files, you'll need
    the  inverse. It is also possible to transform data in MNI space on to the individual
    subject, which also requires the inverse transform. Deformations are saved as .nii
    files, which contain three volumes to encode the x, y and z coordinates.
    """ 
    self.deformation_fields = [1, 1]
    
  @checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
  def setDeformationFieldForwardOutputPath(self, output_path):
    self.forward_deformation_path = output_path

  @checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
  def setDeformationFieldInverseOutputPath(self, output_path):
    self.inverse_deformation_path = output_path
      
  def getStringListForBatch(self):
    batch_list = []
    batch_list.extend(self.channel_container.getStringListForBatch())
    batch_list.extend(self.tissue_container.getStringListForBatch())
    batch_list.append('warp.mrf = %g;' %self.MRF_parameter)
    batch_list.append('warp.reg = %i;' %self.warping_regularisation)
    batch_list.append("warp.affreg = '%s';" %self.affine_regularisation)
    batch_list.append('warp.samp = %g;' %self.sampling_distance)
    batch_list.append('warp.write = %s;' %convertlistToSPMString(self.deformation_fields))
    return self._addSpecificSPMPrefix(batch_list)

  @checkIfArgumentTypeIsAllowed(list, 1)
  def _addSpecificSPMPrefix(self, batch_list):
    spm8_keyword = 'spm.tools.preproc8'
    return addBatchKeyWordInEachItem(spm8_keyword, batch_list)
    
  def _moveSPMDefaultPathsIfNeeded(self):
    self.channel_container.moveBiasSavingIfNeeded()
    self._moveTissuesIfNeeded()
    self._moveDeformationFieldsIfNeeded()
    
  def _moveTissuesIfNeeded(self):
    channel = self.channel_container[0]
    volume_path = channel.getVolumePath()
    if volume_path is not None:
      self.tissue_container.moveTissuesIfNeeded(volume_path)
    else:
      raise ValueError('Volume path not found but is mandatory')  
  
  def _moveDeformationFieldsIfNeeded(self):
    volume_path = self.channel_container[0].getVolumePath()
    if volume_path is not None:
      if self.forward_deformation_path is not None:
        moveSPMPath(volume_path, 
                    self.forward_deformation_path, 
                    prefix=self.forward_deformation_prefix)
      if self.inverse_deformation_path is not None:
        moveSPMPath(volume_path, 
                    self.inverse_deformation_path, 
                    prefix=self.inverse_deformation_prefix)

      