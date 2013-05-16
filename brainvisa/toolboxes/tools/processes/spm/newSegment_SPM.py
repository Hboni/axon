# -*- coding: utf-8 -*-
#  This software and supporting documentation are distributed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
#      Equipe Cogimage
#      UPMC, CRICM, UMR-S975
#      CNRS, UMR 7225
#      INSERM, U975
#      Hopital Pitie Salpetriere
#      47 boulevard de l'Hopital
#      75651 Paris cedex 13
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
from brainvisa.tools.spm_segmentation import \
  initializeUnifiedSegmentationParameters_usingSPM8DefaultValuesForPET, \
  writeUnifiedSegmentationMatFile
from brainvisa.tools.spm_utils import movePathToDiskItem, movePath
import brainvisa.tools.spm_run as spm

configuration = Application().configuration

name = 'new segment (no links between parameters)' # no links between parameters so can be easily used in pipelines
userLevel = 2

def validation():
  return spm.validation(configuration)

# inputs/outputs definition
signature = Signature(
  'MRI_Nat', ReadDiskItem('4D Volume', 'Aims readable volume formats'),
  'analysis', String(),
  'grey_tpm', ReadDiskItem('4D Volume', 'Aims readable volume formats'),
  'spmJobName', String(),
  
  'c_biasreg', Choice(('no regularisation (0)', '0'), ('extremely light regularisation (0.00001)', '0.00001'), ('very light regularisation (0.0001) *SPM default*', '0.0001')
                    , ('light regularisation (0.001)', '0.001'), ('medium regularisation (0.01)', '0.01'), ('heavy regularisation (0.1)', '0.1'), ('very heavy regularisation (1)', '1'), ('extremely heavy regularisation (10)', '10'),),
  'c_biasfwhm', Choice(('30mm cutoff', '30'), ('40mm cutoff', '40'), ('50mm cutoff', '50'), ('60mm cutoff', '60'), ('70mm cutoff', '70'), ('80mm cutoff', '80'), ('90mm cutoff', '90'), ('100mm cutoff', '100'), ('110mm cutoff', '110'), ('120mm cutoff', '120'), ('130mm cutoff', '130'), ('140mm cutoff', '140'), ('150mm cutoff', '150'), ('No correction', 'Inf')),
  'c_write', Choice(('save nothing', '[0 0]'), ("save bias corrected", '[0 1]'), ("save bias field", '[1 0]'), ("save field and corrected", '[1 1]')),
  'biasCorrected', WriteDiskItem('4D Volume', 'Aims readable volume formats'),

  'grey_ngaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'grey_native', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'grey_nat', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'grey_warped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),
  'grey_Mni', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  
  'write_field', Choice(('None', '[0 0]'), ("Inverse", '[1 0]'), ("Forward", '[0 1]'), ("Inverse + Forward", '[1 1]')),
  'deFld', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'invDeFld', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'deFld_segMat', WriteDiskItem('Matlab SPM file', 'Matlab file'),

  'white_ngaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'white_native', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'white_probability', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'white_warped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),

  'csf_ngaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'csf_native', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'csf_probability', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'csf_warped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),

  'bone_ngaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'bone_native', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'bone_probability', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'bone_warped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),

  'softTissuengaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'softTissuenative', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'softTissue_probability', WriteDiskItem('4D Volume', 'Aims readable volume formats'),
  'softTissuewarped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),

  'airAndBackground_ngaus', Choice(('1', '1'), ('2', '2') , ('3', '3'), ('4', '4'), ('5', '5'), ('6', '6'), ('7', '7'), ('8', '8'), ('Nonparametric', 'Inf')),
  'airAndBackground_native', Choice(('None', '[0 0]'), ("Native", '[1 0]'), ("DARTEL Imported", '[0 1]'), ("Native + DARTEL Imported", '[1 1]')),
  'airAndBackground_warped', Choice(('None', '[0 0]'), ("Modulated", '[0 1]'), ("UnModulated", '[1 0]'), ("Modulated + UnModulated", '[1 1]')),

  'w_mrf', String(),
  'w_reg', String(),
  'w_affreg', Choice(('No Affine Registration', """''"""), ("ICBM space template - European brains", """'mni'"""), ("ICBM space template - East Asian brains", """'eastern'"""), ("Average sized template", """'subj'"""), ("No regularisation", """'none'""")),
  'w_samp', String(),
)

def initialization(self):
  self.setOptional('biasCorrected')  
  self.analysis = 'SpmNewSegmentation'
  self.spmJobName = 'newSegment'

  initializeUnifiedSegmentationParameters_usingSPM8DefaultValuesForPET(self)


def execution(self, context):
  print "\n start ", name, "\n"
  context.warning("unified segmentation with SPM can take time, around 7 min")
 
  inDir = self.MRI_Nat.fullPath()  
  inDir = inDir[:inDir.rindex('/')]  
  spmJobFile = inDir + '/' + self.spmJobName+ "_job.m"
      
  matfilePath = writeUnifiedSegmentationMatFile(context, configuration, self.MRI_Nat.fullPath(), spmJobFile
       , self.c_biasreg, self.c_biasfwhm, self.c_write
       , self.grey_tpm, self.grey_ngaus, self.grey_native, self.grey_warped
       , self.grey_tpm, self.white_ngaus, self.white_native, self.white_warped
       , self.grey_tpm, self.csf_ngaus, self.csf_native, self.csf_warped
       , self.grey_tpm, self.bone_ngaus, self.bone_native, self.bone_warped
       , self.grey_tpm, self.softTissuengaus, self.softTissuenative, self.softTissuewarped
       , self.grey_tpm, self.airAndBackground_ngaus, self.airAndBackground_native, self.airAndBackground_warped
       , self.w_mrf, self.w_reg, self.w_affreg, self.w_samp, self.write_field)
    
  spm.run(context, configuration, matfilePath)    
  self.moveSpmOutFiles()
  
  print "\n stop ", name, "\n"



def moveSpmOutFiles(self):
  subjectName = os.path.basename(self.MRI_Nat.fullPath()).partition(".")[0]
  ext = os.path.basename(self.MRI_Nat.fullPath()).partition(".")[2]
  inDir = os.path.dirname(self.MRI_Nat.fullName())
  outDir = os.path.dirname(self.grey_nat.fullName())
  
  imSegUni1 = inDir + "/c1" + subjectName + "." + ext
  movePathToDiskItem(imSegUni1, self.grey_nat)
  imSegUni1 = inDir + "/wc1" + subjectName + "." + ext
  movePathToDiskItem(imSegUni1, self.grey_Mni)
  imSegUni2 = inDir + "/c2" + subjectName + "." + ext
  movePathToDiskItem(imSegUni2, self.white_probability)
  imSegUni3 = inDir + "/c3" + subjectName + "." + ext
  movePathToDiskItem(imSegUni3, self.csf_probability)
  imSegUni4 = inDir + "/c4" + subjectName + "." + ext
  movePathToDiskItem(imSegUni4, self.bone_probability)
  imSegUni5 = inDir + "/c5" + subjectName + "." + ext
  movePathToDiskItem(imSegUni5, self.softTissue_probability)
  
  if self.write_field != '[0 0]':
    imDefField = inDir + "/y_" + subjectName + "." + ext 
    movePathToDiskItem(imDefField, self.deFld)
    imInvDefField = inDir + "/iy_" + subjectName + "." + ext # /iy is inverse deformation field
    movePathToDiskItem(imInvDefField, self.invDeFld)
    trSeg8Mat = inDir + "/" + subjectName + "_seg8.mat"  
    movePathToDiskItem(trSeg8Mat, self.deFld_segMat)
    
  job = inDir + '/' + self.spmJobName+ "_job.m"
  movePath(job, outDir + '/' + self.spmJobName + "_job.m")
  batch = inDir + '/' + self.spmJobName + ".m"
  movePath(batch, outDir + '/' + self.spmJobName + ".m")

  biasCorrected = inDir + "/m" + subjectName + ".nii"
  movePathToDiskItem(biasCorrected, self.biasCorrected)
#New Segment

#This  toolbox  is  currently only work in progress, and is an extension of the default unified segmentation.  
#The algorithm is essentially the same as that described in  the  Unified  Segmentation  paper,  except for 
#(i) a slightly different treatment of the mixing proportions, 
#(ii) the use of an improved registration model, 
#(iii) the ability  to  use  multi-spectral  data,  
#(iv)  an  extended  set of tissue probability maps, which allows a different treatment of voxels outside the brain. 
#Some of the options  in  the  toolbox  do  not yet work, and it has not yet been seamlessly integrated into the SPM8 software.  
#Also, the extended tissue probability maps need further refinement. 
#The current versions were crudely generated (by JA) using data that was kindly provided by Cynthia Jongen of the Imaging Sciences Institute at Utrecht, NL.
#
#This  function segments, bias corrects and spatially normalises - all in the same model.  
#Many investigators use tools within older versions of SPM for a technique that  has  become  known as "optimised" voxel-based morphometry (VBM). 
#VBM performs region-wise volumetric comparisons among populations of subjects. 
#It requires  the  images  to  be  spatially  normalised,  segmented  into  different tissue classes, and smoothed, prior to performing statistical tests. 
#The "optimised" pre-processing  strategy  involved  spatially  normalising  subjects  brain  images to a standard space, by matching grey matter in these images, to a grey matter reference.  The historical motivation behind this approach was to reduce the confounding effects of non-brain (e.g. scalp) structural variability on the registration.
#Tissue  classification  in  older  versions  of  SPM required the images to be registered with tissue probability maps. 
#After registration, these maps represented the prior  probability  of  different  tissue  classes  being  found  at  each  location  in an image.  
#Bayes rule can then be used to combine these priors with tissue type probabilities derived from voxel intensities, to provide the posterior probability.
#
#This procedure was inherently circular, because the registration required an initial tissue classification, and the tissue classification requires an initial registration. 
#This  circularity  is  resolved  here  by  combining  both  components  into  a single generative model. 
#This model also includes parameters that account for image intensity  non-uniformity.  Estimating  the  model  parameters (for a maximum a posteriori solution) involves alternating among classification, bias correction and registration steps. 
#This approach provides better results than simple serial applications of each component.
#
#Note  that  on  a  32 bit computer, the most memory that SPM or any other program can use at any time is 4Gbytes (or sometimes only 2Gbytes).  
#This is because the  largest  number  that  can  be  represented  with  32  bits  is  4,294,967,295,  which limits how much memory may be addressed by any one process.  
#Out of memory errors may occasionally be experienced when trying to work with large images.  
#64-bit computers can usually handle such cases.
#
#This branch contains 3 items:
#* Data
#* Tissues
#* Warping & MRF