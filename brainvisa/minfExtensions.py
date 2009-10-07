# -*- coding: iso-8859-1 -*-

# Copyright IFR 49 (1995-2009)
#
#  This software and supporting documentation were developed by
#      Institut Federatif de Recherche 49
#      CEA/NeuroSpin, Batiment 145,
#      91191 Gif-sur-Yvette cedex
#      France
#
# This software is governed by the CeCILL-B license under
# French law and abiding by the rules of distribution of free software.
# You can  use, modify and/or redistribute the software under the 
# terms of the CeCILL-B license as circulated by CEA, CNRS
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
# knowledge of the CeCILL-B license and that you accept its terms.

'''
Registration of all BrainVISA specific minf formats.

@author: Yann Cointepas
@organization: U{NeuroSpin<http://www.neurospin.org>} and U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

from soma.minf.api import iterateMinf, createMinfWriter, \
                          createReducerAndExpander, registerClass, \
                          registerClassAs

#------------------------------------------------------------------------------
def initializeMinfExtensions():
  createReducerAndExpander( 'brainvisa_2.0', 'minf_2.0' )

  # Logging extensions
  from neuroLog import TextFileLink, LogFileLink, LogFile
  registerClass( 'brainvisa_2.0', TextFileLink, 'TextFileLink' )
  registerClass( 'brainvisa_2.0', LogFileLink,'LogFileLink' )
  registerClass( 'brainvisa_2.0', LogFile.Item, 'LogFile.Item' )
  registerClassAs( 'brainvisa_2.0', LogFile.SubTextLog, TextFileLink )
  createReducerAndExpander( 'brainvisa-log_2.0', 'brainvisa_2.0' )

  from neuroProcesses import ProcessTree
  registerClass( 'brainvisa_2.0', ProcessTree, 'ProcessTree')
  registerClass( 'brainvisa_2.0', ProcessTree.Branch, 'ProcessTree.Branch' )
  registerClass( 'brainvisa_2.0', ProcessTree.Leaf, 'ProcessTree.Leaf' )
  createReducerAndExpander( 'brainvisa-tree_2.0', 'brainvisa_2.0' )

  from brainvisa.history import minfHistory, ProcessExecutionEvent, BrainVISASessionEvent
  registerClass( 'brainvisa_2.0', ProcessExecutionEvent, ProcessExecutionEvent.eventType )
  registerClass( 'brainvisa_2.0', BrainVISASessionEvent, BrainVISASessionEvent.eventType )
  createReducerAndExpander( minfHistory, 'brainvisa_2.0' )

