# -*- coding: utf-8 -*-
import os 
import shutil
import numpy

from soma.spm.custom_decorator_pattern import checkIfArgumentTypeIsAllowed, checkIfArgumentTypeIsStrOrUnicode

@checkIfArgumentTypeIsStrOrUnicode(argument_index=0)
@checkIfArgumentTypeIsAllowed(list, 1)
def addBatchKeyWordInEachItem(key_word, item_list):
  new_item_list = []
  for item in item_list:
    new_item_list.append(key_word + '.' + item)
  return new_item_list
 
@checkIfArgumentTypeIsAllowed(list, 0) 
def convertlistToSPMString(list_to_convert):
  SPM_string_list = [str(item) for item in list_to_convert]
  return '[' + ' '.join(SPM_string_list) + ']'
  
@checkIfArgumentTypeIsAllowed(numpy.ndarray, 0) 
def convertNumpyArrayToSPMString(numpy_array):  
  if numpy_array.size > 1:
    array_list = numpy_array.tolist()
    SPM_row_list = []
    for row in array_list:
      row_str_list = [str(item) for item in row]
      SPM_row_list.append(' '.join(row_str_list))
    SPM_string = '[' + '\n'.join(SPM_row_list) + ']'
  elif numpy_array.size == 1:
    SPM_string = str(numpy_array.mean())#numpy_array.mean() work if [<value>] or [[<value>]], ...
  else:
    raise ValueError("Empty numpy array")
  return SPM_string

@checkIfArgumentTypeIsAllowed(list, 0)
#@checkIfArgumentTypeIsAllowed(bool, 1)
def convertPathListToSPMBatchString(path_list, add_dimension=True):
  if add_dimension:
    SPM_string_template = "'%s,1'"
  else:
    SPM_string_template = "'%s'"
    
  path_list_for_batch = [] 
  for path in path_list:
    path_list_for_batch.append(SPM_string_template % path)
    
  return '\n'.join(path_list_for_batch)
#===============================================================================
# Utils about Paths
#===============================================================================
@checkIfArgumentTypeIsStrOrUnicode(argument_index=0)
@checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
#@checkIfArgumentTypeIsStrOrUnicode(argument_index=2)
#@checkIfArgumentTypeIsStrOrUnicode(argument_index=3)
def moveSPMPath(reference_path, output_path, prefix='', suffix='', extension=None):
  default_spm_path = _addFileNamePrefixAndSuffixToPath(reference_path, prefix, suffix)
  if extension is not None:
    default_spm_path = _replaceFilePathExtension(default_spm_path, extension)
  moveFileAndCreateFoldersIfNeeded(default_spm_path, output_path)
  
@checkIfArgumentTypeIsStrOrUnicode(argument_index=0)
@checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
@checkIfArgumentTypeIsStrOrUnicode(argument_index=2)
def _addFileNamePrefixAndSuffixToPath(path, prefix, suffix):
  current_directory = os.path.dirname(path)
  basename = os.path.basename(path)
  basename_splitted = basename.split('.')
  basename_splitted[0] = prefix + basename_splitted[0] + suffix
  new_basename = '.'.join(basename_splitted)
  new_path = os.path.join(current_directory, new_basename)
  return new_path

@checkIfArgumentTypeIsStrOrUnicode(argument_index=0)
@checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
def _replaceFilePathExtension(path, extension):
  path_splitted = path.split('.')
  path_splitted[-1] = extension
  new_path = '.'.join(path_splitted)
  return new_path

@checkIfArgumentTypeIsStrOrUnicode(argument_index=0)
@checkIfArgumentTypeIsStrOrUnicode(argument_index=1)
def moveFileAndCreateFoldersIfNeeded(source_path, destination_path):
  if not os.path.exists(os.path.dirname(destination_path)):
    os.makedirs(os.path.dirname(destination_path))
  else:
    pass#folder already exists
  shutil.move(source_path, destination_path)