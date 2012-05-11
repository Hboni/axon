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
"""
This module contains classes defining data items that are called **diskItems** in Brainvisa.
These diskItems are associated to files that store data on a filesystem, 
and attributes that are used to index the data in a database.

The main class in this module is :py:class:`DiskItem`. This class is derived into two sub-classes :py:class:`File` for the diskitems that are stored as files and :py:class:`Directory` for the diskitems that represent directories.

A diskItem has a **type** indicating what the data represents (a Volume, a T1 MRI, fMRI data...) and a **format** indicating the file format used to write the data in files.
The class :py:class:`DiskItemType` represents data types.
The class :py:class:`Format` represents file formats.

Two general formats are defined in this module as global variables:
  
.. py:data:: directoryFormat
  
  This format matches any directory.
  
.. py:data:: fileFormat
  
  This format matches any file.
  
The available types and formats in Brainvisa ontology are defined in each toolbox in python files under the ``types`` directory. 
They are loaded at Brainvisa startup using the function :py:func:`readTypes` and stored in global maps:
  
  .. py:data:: formats
  
    A global map which associates each format id to the matching object :py:class:`Format`.

  .. py:data:: formatLists
  
    A global map which associates each list of formats id to the matching object :py:class:`NamedFormatList`.
  
  .. py:data:: diskItemTypes
  
    A global map which associates each type id to the matching object :py:class:`DiskItemType`
    
  .. py:data:: mef
    
    A global instance of the class :py:class:`TypesMEF` which is used to read the Brainvisa ontology types files.

The following function are available to get a format or a type object from its id:
  
  * :py:func:`getFormat`
  * :py:func:`getFormats`
  * :py:func:`getAllFormats`
  * :py:func:`getDiskItemType`
  * :py:func:`getAllDiskItemTypes`
  * :py:func:`isSameDiskItemType`

This module also defines classes for temporary files and directories: :py:class:`TemporaryDiskItem`, :py:class:`TemporaryDirectory`. 
The function :py:func:`getTemporary` enables to create a new temporary item.
All temporary files and directories are written in Brainvisa global temporary directory which can be choosen in Brainvisa options. The diskitem corresponding to this global directory is stored in a global variable:

.. py:data:: globalTmpDir

  It is the default parent diskItem for temporary diskitems.
  

:Inheritance diagram:

.. inheritance-diagram:: neuroDiskItems

:Classes:

"""
import types, sys, os, errno, stat, cPickle, operator, time, traceback
from weakref import ref, WeakValueDictionary
from UserList import UserList
from threading import RLock

from soma.html import htmlEscape
from soma.undefined import Undefined
from soma.uuid import Uuid
from soma.path import split_path
from soma.minf.api import readMinf, MinfError
from soma.wip.application.api import Application

import neuroConfig
from brainvisa.processes.neuroException import *
from brainvisa.data import temporary
from brainvisa.data.patterns import DictPattern
from brainvisa import shelltools
from brainvisa.multipleExecfile import MultipleExecfile
from PyQt4.QtCore import QObject, SIGNAL

#----------------------------------------------------------------------------
def sameContent( a, b ):
  """
  Checks if *a* and *b* have the same content.
  
  If the two objects are lists, the function is called on each element of the list.
  If the first object has a method named *sameContent*, it is called. 
  Else, the result of a comparison with *==* operator is returned.
  """
  result = 0
  if type( a ) is type( b ):
    if type( a ) in ( types.ListType, types.TupleType ):
      result = 1
      i = 0
      for x in a:
        if not sameContent( x, b[i] ):
          result = 0
          break
        i += 1
    elif hasattr( a, 'sameContent' ):
      result = a.sameContent( b )
    else:
      result = a == b
  return result


#----------------------------------------------------------------------------
def modificationHashOrEmpty( f ):
  """
  Returns a tuple containing information about the file from :py:func:`os.lstat`.
  Returns an empty tuple if an exception occurs. 
  """
  try:
    s = os.lstat( f )
    return ( s.st_mode, s.st_uid, s.st_gid, s.st_size, s.st_mtime, s.st_ctime )
  except OSError:
    return ()


#----------------------------------------------------------------------------
class DiskItem(QObject):
  """
  This class represents data stored in one or several files on a filesystem.
  It can have additional information stored in attributes and may be indexed in a Brainvisa database.
  
  A diskItem can have hierarchy attributes (comes from Brainvisa ontology), 
  minf attributes (that are stored in a .minf file), and possibly other attributes.
  Several methods enable to access the values of the attributes: :py:meth:`getHierarchy`, :py:meth:`getNonHierarchy`, :py:meth:`attributes`, :py:meth:`globalAttributes`, :py:meth:`hierarchyAttributes`, :py:meth:`localAttributes`, :py:meth:`minf`. 
  The attributes of a diskItem can also be requested using the dictionary notation ``d["attribute_name"]``, which calls :py:meth:`get` method. 
  
  A diskItem can be identified by a unique identifier called *uuid*. This uuid is stored in minf attributes and it is an instance of :py:class:`soma.uuid.Uuid` class.
  
  Several methods enable to request the name of the files associated to the diskItem: :py:meth:`fullPath`, :py:meth:`fullName`,  :py:meth:`fullPaths`, :py:meth:`fullPathSerie`, :py:meth:`fullPathsSerie`.
  
  :Attributes:

  .. py:attribute:: name
  
    name of the diskItem, generally the filename of the first file.

  .. py:attribute:: parent
  
    a parent diskItem, generally the diskItem associated to the directory that contains the data files of this diskItem.

  .. py:attribute:: type
  
    Data type of the diskItem, indicating the meaning of the data. It is an instance of :py:class:`DiskItemType`.

  .. py:attribute:: format
  
    DiskItem file format, indicated by the files extensions. It is an instance of :py:class:`Format`.

  :Methods:

  """
  
  _minfLock=RLock()
  
  def __init__( self, name, parent ):
    super(DiskItem, self).__init__()
    self.name = name
    if name and name[ -5: ] != '.minf': 
      self._files = [ name ]
    else: self._files = []
    self.parent = parent
    if self.parent is None:
      self._topParentRef = ref( self )
    else:
      self._topParentRef = self.parent._topParentRef
    self._localAttributes = {}
    self._globalAttributes = {}
    self._minfAttributes = {}
    self._otherAttributes = {}
    self.type = None
    self.format = None
    self._setLocal( 'name_serie', [] )
    self._isTemporary = 0
    self._uuid = None
    self._write = False
    self._identified = False
    self._lock = RLock()
  
  
  def __getstate__( self ):
    state =  {
      'name': self.name,
      '_files': self._files,
      'parent': self.parent,
      '_localAttributes': self._localAttributes,
      '_globalAttributes': self._globalAttributes,
      '_minfAttributes': self._minfAttributes,
      '_otherAttributes': self._otherAttributes,
      '_uuid': self._uuid,
      '_write': self._write,
      '_identified': self._identified,
    }
    if self.type:
      state[ 'type' ] = self.type.id
    else:
      state[ 'type' ] = None
    if self.format:
      state[ 'format' ] = self.format.id
    else:
      state[ 'format' ] = None
    priority = getattr( self, '_priority', None )
    if priority is not None:
      state[ '_priority' ] = priority
    return state


  def __setstate__( self, state ):
    self.name = state[ 'name' ]
    self._files = state[ '_files' ]
    self.parent = state[ 'parent' ]
    if self.parent is None:
      self._topParentRef = ref( self )
    t = state[ 'type' ]
    if t: self.type = getDiskItemType( t )
    else: self.type = None
    t = state[ 'format' ]
    if t: self.format = getFormat( t )
    else: self.format = None
    self._localAttributes = state[ '_localAttributes' ]
    self._globalAttributes = state[ '_globalAttributes' ]
    self._minfAttributes = state[ '_minfAttributes' ]
    self._otherAttributes = state[ '_otherAttributes' ]
    self._isTemporary = 0
    priority = state.get( '_priority' )
    if priority is not None:
      self._priority = priority
    self._changeUuid( state.get( '_uuid' ) )
    self._write = state[ '_write' ]
    self._identified = state[ '_identified' ]
    if not hasattr( self, '_lock' ):
      self._lock = RLock()
  
  
  def __eq__( self, other ):
    if isinstance( other, basestring ):
      return other in self.fullPaths()
    return self is other or ( isinstance( other, DiskItem ) and self.fullPath() == other.fullPath() )


  def __ne__( self, other ):
    if isinstance( other, basestring ):
      return other not in self.fullPaths()
    return self is not other and ((not isinstance( other, DiskItem )) or self.fullPath() != other.fullPath() )
  
  
  def clone( self ):
    """
    Returns a deep copy of the current diskItem object. The associated files are not copied. 
    """
    result = self.__class__( self.name, self.parent )
    result.__setstate__( self.__getstate__() )
    # Copy attributes so that they can be modified without
    # changing cloned item attributes
    self.copyAttributes( result )
    return result
  
  
  def _topParent( self ):
    try:
      return self._topParentRef()
    except AttributeError:
      pass
    if self.parent:
      p = self.parent
      while p.parent is not None:
        p = p.parent
      self._topParentRef = p._topParentRef
    else:
      self._topParentRef = ref( self )
    return self._topParentRef()
  
  
  def attributes( self ):
    """
    Returns a dictionary containing the name and value of all the attributes associated to this diskItem.
    """
    result = {}
    self._mergeAttributes( result )
    return result


  def globalAttributes( self ):
    """
    Returns a dictionary containing the name and value of the global attributes. 
    They may come from parent diskItem and are transmitted to child diskItem.
    The global attributes are the attributes coming from data ontoloy.
    """
    result = {}
    self._mergeGlobalAttributes( result )
    return result
  
  
  def localAttributes( self ):
    """
    Returns a dictionary containing the name and value of the local attributes. 
    The local attributes are valid only for this diskItem, they are no transmitted to child diskItem. 
    For example the attribute ``name_serie`` that indicates the list of numbers of a serie of files that store the data, is a local attribute.
    """
    result = {}
    self._mergeLocalAttributes( result )
    return result


  def copyAttributes( self, other ):
    """
    Copy all the attributes of the given diskItem in the current diskItem.
    
    :param other: the :py:class:`DiskItem` whose attributes will be copied.
    """
    self._localAttributes = other._localAttributes.copy()
    self._globalAttributes = other._globalAttributes.copy()
    self._minfAttributes = other._minfAttributes.copy()
    self._otherAttributes = other._otherAttributes.copy()
  
  
  def _mergeAttributes( self, result ):
    """
    Updates result dictionary with all attributes of the current diskItem and its parent global attributes.
    """
    result.update( self._globalAttributes )
    if self.parent:
      self.parent._mergeAttributes( result )
    result.update( self._localAttributes )
    result.update( self._otherAttributes )
    result.update( self._minfAttributes )


  def _mergeGlobalAttributes( self, result ):
    """
    Updates result dictionary with all global attributes of the current diskItem and its parent global attributes.
    """
    result.update( self._globalAttributes )
    if self.parent:
      self.parent._mergeGlobalAttributes( result )


  def _mergeLocalAttributes( self, result ):
    if self.parent:
      self.parent._mergeLocalAttributes( result )
    result.update( self._localAttributes )
    result.update( self._otherAttributes )
    result.update( self._minfAttributes )


  def _mergeHierarchyAttributes( self, result ):
    if self.parent:
      self.parent._mergeHierarchyAttributes( result )
    result.update( self._localAttributes )
    result.update( self._globalAttributes )


  def _mergeNonHierarchyAttributes( self, result ):
    if self.parent:

      self.parent._mergeNonHierarchyAttributes( result )
    result.update( self._otherAttributes )
    result.update( self._minfAttributes )


  def fileName( self, index=0 ):
    """
    Returns the filename of the file number index in the list of files associated to this diskItem.
    The absolute files paths are generally stored in diskItems. So this function is equivalent to :py:meth:`fullPath`.
    """
    name_serie = self.get( 'name_serie' )
    if name_serie:
      return self.fileNameSerie( index / len( self._files ) , 
                                 index % len( self._files ) )
    if self._files: return self._files[index]
    else: return self.name


  def fileNames( self ):
    """
    Returns the list of filenames of the files associated to this diskItem. 
    The absolute files paths are generally stored in diskItems. So this function is equivalent to :py:meth:`fullPaths`.
    """
    name_serie = self.get( 'name_serie' )
    if name_serie:
      result = []
      for number in name_serie:
        result += map( lambda x, number=number: expand_name_serie( x, number ),
                       self._files )
      return result
    if self._files: return self._files
    else: return [ self.name ]
  
  
  def fileNameSerie( self, serie, index=0 ):
    """
    This function can be used only on diskItems that are a serie of data. For example a serie of 3D volumes, each volume being stored in two files .ima and .dim (GIS format). 
    Returns the name of the index number file of the serie number item of the serie.
    The absolute files paths are generally stored in diskItems. So this function is equivalent to :py:meth:`fullPathSerie`.
    
    :param int serie: index of the item in the serie.
    :param int index: index of the file in one item of the serie.
    """
    name_serie = self.get( 'name_serie' )
    if name_serie:
      return expand_name_serie( self._files[ index ],
                                name_serie[ serie ] )
    raise RuntimeError( HTMLMessage(_t_( '<em>%s</em> is not a file series' )) )
  
  
  def fileNamesSerie( self, serie ):
    """
    Returns all the files of one item of the serie. 
    The absolute files paths are generally stored in diskItems. So this function is equivalent to :py:meth:`fullPathsSerie`.
    
    :param int serie: index of the item in the serie.
    """
    name_serie = self.get( 'name_serie' )
    if name_serie:
      return map( lambda x, number=name_serie[serie]: expand_name_serie( x, number ),
                   self._files )
    raise RuntimeError( HTMLMessage(_t_( '<em>%s</em> is not a file series' )) )
  
  
  def fullName( self ):
    """
    Returns the absolute path to the diskItem with its name (without extension). 
    """
    if self.parent is None:
      return self.name
    else:
      return os.path.join( self.parent.fullName(), self.name )

  def relativePath(self, index=0):
    """
    Gets the file path of this diskitem, relatively to the path its database directory.
    If there is no database information in the attributes it returns the full path of the diskItem.
    """
    database=self.get("database")
    if database is None:
      database=self.get("_database")
    path=self.fullPath(index)
    if database and path.startswith( database ):
      path=path[ len(database) +1: ]
    return path
    
  def fullPath( self, index=0 ):
    """
    Returns the absolute file name of the index number file of the diskItem.
    """
    if self.parent is None:
      return self.fileName(index)
    else:
      return os.path.join( self.parent.fullPath(), self.fileName(index) )


  def fullPaths( self ):
    """
    Returns the absolute file names of the all the files of the diskItem.
    """
    if self.parent is None:
      return self.fileNames()
    else:
      return map( lambda x, p=self.parent.fullPath(): os.path.join( p, x ),
                  self.fileNames() )

  def existingFiles(self):
    """
    Returns all files associated to this diskitem, that really exist plus its minf file if it exists.
    """
    files=[ f for f in self.fullPaths() if os.path.exists(f)]
    minfFile= self.minfFileName()
    if minfFile != self.fullPath() and os.path.exists(minfFile):
      files.append(minfFile)
    return files

  def fullPathSerie( self, serie, index=0 ):
    """
    This function can be used only on a diskItem that is a serie of data. For example a serie of 3D volumes, each volume being stored in two files *.ima* and *.dim* (*GIS format*). 
    Returns the absolute file name of the index number file of the serie number item of the serie.
    
    :param int serie: index of the item in the serie.
    :param int index: index of the file in one item of the serie.
    """

    if self.parent is None:
      return self.fileNameSerie( serie, index )
    else:
      return os.path.join( self.parent.fullPath(), self.fileNameSerie( serie, index ) )


  def fullPathsSerie( self, serie ):
    """
    This function can be used only on a diskItem that is a serie of data. For example a serie of 3D volumes, each volume being stored in two files *.ima* and *.dim* (*GIS format*). 
    Returns all the absolute file names of the serie number item of the serie.
    
    :param int serie: index of the item in the serie.
    """

    if self.parent is None:
      return self.fileNamesSerie( serie )
    else:
      return map( lambda x, p=self.parent.fullPath(): os.path.join( p, x ),
                  self.fileNamesSerie( serie ) )


  def firstFullPathsOfEachSeries( self ):
    """
    Returns the first file name of each item of the serie.
    """
    return map( lambda i, self=self: self.fullPathSerie( i ),
                range( len( self.get( 'name_serie' ) ) ) )


  def _getGlobal( self, attrName, default = None ):
    r = self._globalAttributes.get( attrName )
    if r is None:
      if self.parent: return self.parent._getGlobal( attrName, default )
      else: return default
    return r


  def _getLocal( self, attrName, default = None ):
    r = self._localAttributes.get( attrName )
    if r is None:
      if self.parent: return self.parent._getLocal( attrName, default )
      else: return default
    return r


  def _getOther( self, attrName, default = None ):
    r = self._otherAttributes.get( attrName )
    if r is None:
      if self.parent: return self.parent._getOther( attrName, default )
      else: return default
    return r
  
  
  def get( self, attrName, default = None, search_header=False ):
    """
    Gets the value of an attribute. 
    If the attribute is not found in the attributes stored in this diskItem object, 
    it can be searched for in data file header using :py:func:`aimsFileInfo` function if the option search_header is True.
    
    .. warning::
      If search_header is True, the method can take more time to execute than :py:func:`getHierarchy` and :py:func:`getNonHierarchy`
      when the attribute is not found because it reads the header of the file on the filesystem.
    
    :param string attrName: name of the attribute
    :param default: value returned if the attribute is not set in this diskItem.
    """
    r = self._globalAttributes.get( attrName )
    if r is None: r = self._minfAttributes.get( attrName )
    if r is None: r = self._otherAttributes.get( attrName )
    if r is None: r = self._localAttributes.get( attrName )
    if r is None and search_header:
      info = aimsFileInfo( self.fullPath() )
      for k, v in info.iteritems():
        self._otherAttributes.setdefault( k, v )
      r = info.get( attrName )
    if r is None:
      if self.parent: return self.parent.get( attrName, default )
      else: return default
    return r

  def __getitem__( self, attrName ):
    """
    Enables to use d[attrname] notation.
    """
    r = self.get( attrName )
    if r is None: raise KeyError( attrName )
    return r


  def getInTree( self, attrPath, default = None, separator = '.' ):
    """
    This function could be used to get an attribute value from an object that is an attribute value of a diskItem. 
    
    ::
      
      d.getInTree("attr1.attr2...") <=> d.get(attr1).get(attr2)...

    :param string attrPath: the attributes path, each attribute is separated by a separator character
    :param default: default value is the attribute value is not found
    :param string separator: character separator used to separate the different attributes in the attributes path.
    """
    item = self
    stack = attrPath.split( separator )
    while stack and item is not None:
      item = item.get( stack.pop(0) )
    if item is None or stack:
      return default
    return item
  
  
  def getHierarchy( self, attrName, default = None ):
    """
    Gets the attribute from the global attributes or the local attributes or the parent  hierarchy attributes.
    
    :param string attrName: name of the attribute
    :param default: value returned if the attribute is not found.
    """
    r = self._globalAttributes.get( attrName )
    if r is None: r = self._localAttributes.get( attrName )
    if r is None:
      if self.parent: return self.parent.getHierarchy( attrName, default )
      else: return default
    return r


  def getNonHierarchy( self, attrName, default = None ):
    """
    Gets the attribute value from the attributes written in minf file or in other attributes or in parent non hierarchy attributes.
    
    :param string attrName: name of the attribute
    :param default: value returned if the attribute is not found.
    """
    r = self._minfAttributes.get( attrName )
    if r is None: r = self._otherAttributes.get( attrName )
    if r is None:
      if self.parent: return self.parent.getNonHierarchy( attrName, default )
      else: return default
    return r
  
  
  def nonHierarchyAttributes( self ):
    """
    Returns all non hierarchy attributes in a dictionary.
    """
    result = {}
    self._mergeNonHierarchyAttributes( result )
    return result
  
  
  def hierarchyAttributes( self ):
    """
    Returns all hierarchy attributes as a dictionary.
    """
    result = {}
    self._mergeHierarchyAttributes( result )
    return result
  
  
  def has_key( self, attrName ):
    """
    Returns True if the diskItem has an attribute with this name in one of its dictionaries of attributes.
    """
    r = self._minfAttributes.has_key( attrName )
    if r: return r
    r = self._otherAttributes.has_key( attrName )
    if r: return r
    r = self._localAttributes.has_key( attrName )
    if r: return r
    r = self._globalAttributes.has_key( attrName )
    if r: return r
    if self.parent: return self.parent.has_key( attrName )
    return 0
  
  
  def _setGlobal( self, attrName, value ):
    if self.parent and self.parent._getGlobal( attrName ) is not None:
      raise AttributeError( HTMLMessage(_t_('a global attribute <em>%s</em> already exists in item <em><code>%s</code></em>') % ( str(attrName), str(self) )) )
    self._globalAttributes[ attrName ] = value
  
  
  def _updateGlobal( self, dict ):
    for attrName, value in dict.items():
      self._setGlobal( attrName, value )
  
  
  def _setLocal( self, attrName, value ):
    if self._getGlobal( attrName ) is not None:
      raise AttributeError( HTMLMessage(_t_('a global attribute <em>%s</em> already exists in item <em><code>%s</code></em>') % ( str(attrName), str(self) )) )
    self._localAttributes[ attrName ] = value
  
  
  def _updateLocal( self, dict ):
    for attrName, value in dict.items():
      self._setLocal( attrName, value )
  
  
  def _setOther( self, attrName, value ):
    if self._getGlobal( attrName ) is not None:
      raise AttributeError( HTMLMessage(_t_('a global attribute <em>%s</em> already exists in item <em><code>%s</code></em>') % ( str(attrName), str(self) )) )
    minfValue = self._minfAttributes.get( attrName, Undefined )
    if minfValue is Undefined:
      self._otherAttributes[ attrName ] = value
    elif minfValue != value:
      raise AttributeError( HTMLMessage(_t_('a MINF attribute <em>%s</em> already exists in item <em><code>%s</code></em>') % ( str(attrName), str(self) )) )

  
  def _updateOther( self, dict ):
    for attrName, value in dict.items():
      self._setOther( attrName, value )


  def setMinf( self, attrName, value, saveMinf = True ):
    """
    Adds this attribute to the minf attributes of the diskItem.
    
    :param string attrName: name of the attribute
    :param value: value sets for the attribute
    :param bool saveMinf: if True the modified attributes are written to the diskItem minf file.
    """
    self._otherAttributes.pop( attrName, None )
    self._minfAttributes[ attrName ] = value
    if saveMinf:
      minf = self._readMinf()
      if minf is None: minf = {}
      minf[ attrName ] = value
      self._writeMinf( minf )
  
  
  def minf( self ):
    """
    Returns a dictionary containing the minf attributes of the diskItem.
    """
    return self._minfAttributes
  
  
  def updateMinf( self, dict, saveMinf = True ):
    """
    Adds new attributes to the minf attributes of the diskItem and possibly save the attributes in a minf file.
    
    :param dict: dictionary containing the attributes that will be added to this diskItem minf attributes.
    :param bool saveMinf: if True the minf attributes will be saved in the minf file of the diskItem.
    """
    for attrName, value in dict.items():
      self._otherAttributes.pop( attrName, None )
      self._minfAttributes[ attrName ] = value
    if saveMinf:
      minf = self._readMinf()
      if minf is None: minf = {}
      minf.update( dict )
      self._writeMinf( minf )
  
  
  def isReadable( self ):
    """
    Returns True if all the files associated to this diskItem exist and are readable.
    """
    result = 1
    for p in self.fullPaths():
      if not os.access( p, os.F_OK + os.R_OK ):
        result = 0
        break
    return result


  def isWriteable( self ):
    """
    Returns True if all the files associated to this diskItem exist and are readable and writable.
    """
    result = 1
    for p in self.fullPaths():
      if not os.access( p, os.F_OK + os.R_OK + os.W_OK ):
        result = 0
        break
    return result


  def __repr__( self ):
    return repr( self.fullPath() )


  def childs( self ):
    """
    Virtual function, returns None. It is overriden in derived class :py:class:`Directory`.
    """
    return None
  
  
  def __str__( self ):
    return self.fullPath()


  def setFormatAndTypeAttributes( self, writeOnly=0 ):
    """
    Adds the diskItem format attributes to its other attributes, 
    and the diskItem types attributes to its minf attributes.
    
    :param bool writeOnly: if False the minf file is read to update the dictionary of minf attributes.
    :returns: current disktem
    """
    # Set format attributes
    if self.format is not None:
      self.format.setAttributes( self, writeOnly=writeOnly )
    # Set type attributes
    if self.type is not None:
      self.type.setAttributes( self, writeOnly=writeOnly )
    if not writeOnly:
      # Set local file attributes
      self.readAndUpdateMinf()
    return self


  def priority( self ):
    """
    Returns the value of priority attribute if found 
    else the parent diskItem priority, 
    else the default priority attribute, else 0.
    
    This priority is an attribute associated to the ontology rule that enabled to identify the diskItem.
    """
    if getattr( self, '_priority', None ) is not None:
      return self._priority
    if self.parent is not None:
      return self.parent.priority()
    return getattr( self, '_defaultPriority', 0 )

  
  def setPriority( self, newPriority, priorityOffset=0 ):
    """
    Sets a value for the priority attribute: ``newPriority + priorityOffset``.
    """
    self._priority = newPriority
    if priorityOffset:
      self._priority = self.priority() + priorityOffset
  
  
  def minfFileName( self ):
    """
    Returns the name of the minf file associated to this diskItem. It is generally the name of the main file of the diskItem + the *.minf* extension.
    """
    if self.format is not None and ( isinstance( self.format, MinfFormat ) or self.format.name == 'Minf' ):
      return self.fullPath()
    else:
      return self.fullPath() + '.minf'
  
  
  def saveMinf( self, overrideMinfContent = None ):
    """
    Writes minf attributes or the content of overrideMifContent if given to the minf file.
    """
    minfContent = {}
    if self._uuid is not None:
      minfContent[ 'uuid' ] = str( self._uuid )
    if overrideMinfContent is None:
      minfContent.update( self._minfAttributes )
    else:
      minfContent.update( overrideMinfContent )
    if self._isTemporary: temporary.manager.registerPath( self.minfFileName() )
    self._writeMinf( minfContent )
  
  
  def removeMinf( self, attrName, saveMinf = True ):
    """
    Remove the attribute from minf attributes. 
    
    :param string attrName: name of the attribute to remove
    :param bool saveMinf: if True, the minf attributes are saved in the minf file.
    """
    del self._minfAttributes[ attrName ]
    if saveMinf:
      minf = self._readMinf()
      if minf is not None:
        if minf.pop( attrName, Undefined ) is not Undefined:
          self._writeMinf( minf )
  
  
  def clearMinf( self, saveMinf = True  ):
    """
    Deletes all minf attributes. Also removes the minf file if saveMinf is True.
    """
    self._minfAttributes.clear()
    if saveMinf:
      minf = self.minfFileName()
      if os.path.exists( minf ):
        os.remove( minf )
  
  
  def _readMinf( self ):
    """
    Reads the minf file and returns its content.
    """
    attrFile = self.minfFileName()
    if os.path.exists( attrFile ):
      try:
        f = open( attrFile )
        minfContent = readMinf( f )[ 0 ]
        # Ignor huge DICOM information produced by NMR 
        # and stored in 'dicom' key.
        if minfContent: minfContent.pop( 'dicom', None )
        f.close()
        return minfContent
      except:
        showException( beforeError = \
                       _t_('in file <em>%s</em><br>') % attrFile )
    return None
  
  
  def _writeMinf( self, minfContent ):
    """
    Writes the given content to the minf file.
    if minfContent is None, removes the minf file.
    """
    minf = self.minfFileName()
    if minfContent:
      file = open( minf, 'w' )
      print >> file, 'attributes = ' + repr( minfContent )
      file.close()
    else:
      if os.path.exists( minf ):
        os.remove( minf )
  
  
  def readAndUpdateMinf( self ):
    """
    Reads the content of the minf file and updates the minf attribute dictionary accordingly.
    """
    self._lock.acquire()
    try:
      attrs = self._readMinf()
      if attrs is not None:
        if attrs.has_key( 'uuid' ):
          self._changeUuid( Uuid( attrs[ 'uuid' ] ) )
          del attrs[ 'uuid' ]
        self.updateMinf( attrs, saveMinf=False )
    finally:
      self._lock.release()
  
  
  def createParentDirectory( self ):
    """
    According to the file path of the diskItem, creates the directory that should contain the files if it doesn't exist.
    """
    p = os.path.dirname( self.fullPath() )
    if not os.path.exists( p ):
      try:
        os.makedirs( p )
      except OSError, e:
        if not e.errno == errno.EEXIST:
          # filter out 'File exists' exception, if the same dir has been created
          # concurrently by another instance of BrainVisa or another thread
          raise


  def isTemporary( self ):
    """
    Returns True if it is a temporary diskItem, that is to say its files will be automatically deleted when its is no more referenced.
    """
    return self._isTemporary


  def distance( self, other ):
    '''Returns a value that represents a sort of distance between two DiskItems.
       The distance is not a number but distances can be sorted, it is a tuple of numbers.'''
    # Count the number of common hierarchy attributes
    hierarchyCommon = \
      reduce( 
        operator.add, 
        map( 
          lambda nv, other=other: other.getHierarchy( nv[ 0 ] ) == nv[ 1 ], 
          self.hierarchyAttributes().items() ),
        self.type is other.type )
    # Count the number of common non hierarchy attributes
    nonHierarchyCommon = \
      reduce( 
        operator.add, 
        map( 
          lambda nv, other=other: other.getNonHierarchy( nv[ 0 ] ) == nv[ 1 ], 
          self.nonHierarchyAttributes().items() ),
        self.type is other.type )
    return ( -hierarchyCommon, self.priority() - other.priority(), -nonHierarchyCommon, )


  def _changeUuid( self, newUuid ):
    self._uuid = newUuid
    if newUuid is not None:
      _uuid_to_DiskItem[ newUuid ] = self
  

  def setUuid( self, uuid, saveMinf=True ):
    """
    Sets a new uuid to this diskItem.
    """
    self._changeUuid( Uuid( uuid ) )
    if saveMinf:
      attrs = self._readMinf()
      if not attrs:
        attrs = {}
      if attrs.get( 'uuid' ) != self._uuid:
        attrs[ 'uuid' ] = self._uuid
        try:
          self._writeMinf( attrs )
        except Exception, e:
          raise MinfError( unicode( _t_( 'uuid cannot be saved in minf file' ) + ': ' ) + unicode( e ) )
  
  
  def uuid( self, saveMinf=True ):
    """
    Gets the uuid of the diskItem.
    """
    if self._uuid is None:
      self._minfLock.acquire()
      try:
        attrs = self._readMinf()
        if attrs and attrs.has_key( 'uuid' ):
          self._changeUuid( Uuid( attrs[ 'uuid' ] ) )
        else:
          self.setUuid( Uuid(), saveMinf=saveMinf )
      finally:
        self._minfLock.release()
    return self._uuid
  
  
  def findFormat(self, amongFormats=None):
    """
    Find the format of this diskItem : the format whose pattern matches this diskitem's filename.
    Does nothing if this item has already a format. 
    Doesn't take into account format whose pattern matches any filename (*). 
    Stops when a matching format is found :
    
    * item name is modified (prefix and suffix linked to the format are deleted)
    * item list of files is modified accoding to format patterns
    * the format is applied to the item
    * :py:meth:`setFormatAndTypeAttributes` method is applied.
    """
    if not self.format:
      if not amongFormats:
        amongFormats=getAllFormats()
      for format in amongFormats:
        # don't choose a FormatSeries, we can't know if it is a serie only with the filename, FormatSeries has the same pattern as its base format.
        if not isinstance(format, FormatSeries) and "*" not in format.getPatterns().patterns: # pass formats that match any pattern, it can't be used to find the format only with filename. To be used only contextually in hierarchy rules.
          m = format.match( self )
          if m:
            self.name = format.formatedName( self, m )
            self._files = format.unmatch( self, m )
            format.setFormat( self )
            self.setFormatAndTypeAttributes()
            break;

  def modificationHash( self ):
    """
    Return a value that can be used to assess modification of this 
    DiskItem. Two calls to modificationHash will return the same value if and 
    only if all files in the DiskItem have not changed. Note that the contents 
    of the files are not read, the modification hash rely only on os.stat.
    
    This method uses the funciton :py:func:`modificationHashOrEmpty` to create a hash code for each file.
    """
    files = self.fullPaths() + [ self.minfFileName() ]
    return tuple( [(f,) + tuple(modificationHashOrEmpty( f )) for f in files] )
  
  
  def eraseFiles( self ):
    """
    Deletes all files associated to this diskItem.
    """
    for fp in self.fullPaths():
      if os.path.exists( fp ):
        shelltools.rm( fp )
    fp = self.minfFileName()
    if os.path.exists( fp ):
      shelltools.rm( fp )
  
  
  def isLockData( self):
    """
    Return True or False to know if a file is locked.
    """
    #print "-- FUNCTION isLockData : neuroDiskItems -- "
    nameFileLock = str(self.fileName())  + ".lock"
    #print "File to test"
    #print nameFileLock
    return os.path.isfile( nameFileLock )


  def lockData( self):
    """
    function to lock file
    add a filename.lock file if the filename exists
    """
    #print "-- FUNCTION lockData : neuroDiskItems -- "
    nameFileLock = str(self.fileName())  + ".lock"
    if os.path.isfile( self.fileName()) :
        #print "File to lock" + nameFileLock
        fd = open(nameFileLock, 'a')
        fd.close()
        self.emit(SIGNAL("lockChanged"), True)
        return(True)
    else : return(False)   
    
    
  def unlockData( self):
    """
    function to unlock file
    remove a .lock file 
    """
    #print "-- FUNCTION unlockData : neuroDiskItems -- "

    nameFileLock = str(self.fileName())  + ".lock"
    if os.path.isfile( nameFileLock ) :
        self.emit(SIGNAL("lockChanged"), False)
        fd = os.remove(nameFileLock)

  
  
  
  
#----------------------------------------------------------------------------
class File( DiskItem ):
  """
  This class represents a diskItem that cannot contain other diskItems (it is not a directory).
  """
  def __init__( self, name, parent ):
    DiskItem.__init__( self, name, parent )



#----------------------------------------------------------------------------
class Directory( DiskItem ):
  """
  This class represents a directory, that is to say a diskItem that can contain other diskItems.
  """
  def __init__( self, name, parent ):
    DiskItem.__init__( self, name, parent )
    self._childs = []
    self.lastModified = 0
    self.scanner = None
    if self.parent is None:
      self._automatic_update = True
      self._check_directory_time_only = False
  
  def __getstate__( self ):
    state = DiskItem.__getstate__( self )
    state[ '_childs' ] = self._childs
    state[ 'lastModified' ] = self.lastModified
    state[ 'scanner' ] = self.scanner
    return state
  
  
  def __setstate__( self, state ):
    DiskItem.__setstate__( self, state )
    self._childs = state[ '_childs' ]
    self.lastModified = state[ 'lastModified' ]
    self.scanner = state[ 'scanner' ]
    if self.parent is None:
      self._automatic_update = True
      self._check_directory_time_only = False
  
  
  def childs( self ):
    """
    Returns the children diskItems scaning the directory using the ontology rules.
    Not used.
    """
    self._lock.acquire()
    try:
      if self.scanner is None: return []
      if not self._topParent()._automatic_update:
        return self._childs
      fullName = self.fullPath()
      if not os.path.isdir( fullName ):
        return []
      
      currentTime = int( time.time() )
      listdir = None
      if not self._topParent()._check_directory_time_only:
        modificationTime = 0
        #print 'directory', fullName, 'NOT smart'
        #sys.stdout.flush()
        listdir = []
        for n in os.listdir( fullName ):
          try:
            modificationTime = max( modificationTime, 
                        os.stat( os.path.join( fullName, n ) )[ stat.ST_MTIME ] )
            listdir.append( n )
          except:
            pass
      else:
        modificationTime = os.stat( fullName )[ stat.ST_MTIME ]
  
      debug = neuroConfig.debugHierarchyScanning
      if modificationTime >= self.lastModified:
        if debug:
          print >> debug, '----------------------------------------------'
          print >> debug, fullName, 'modified'
          print >> debug, '----------------------------------------------'
          print >> debug, 'modification time:', time.ctime( modificationTime )
          print >> debug, 'last modification:', time.ctime( self.lastModified )
          debug.flush()
        # Rescan directory
        childs = []
        if listdir is None: listdir = os.listdir( fullName )
        for n in listdir:
          if os.path.isdir( os.path.join( fullName, n ) ):
            childs.append( Directory( n, self ) )
          else:
            childs.append( File( n, self ) )
        if debug:
          print >> debug, 'children count:', len( childs )
        # Identify files
        if self.scanner:
          if self._childs:
            oldChilds={}
            for i in self._childs:
              oldChilds[ ( i.fileName(), i.type, i.format ) ] = i
            self._childs = self.scanner.scan( childs )
            for i in self._childs:
              old = oldChilds.get( ( i.fileName(), i.type, i.format )  )
              if isinstance( old, Directory ):
                i.lastModified = old.lastModified            
                i._childs = old._childs
          else :
            self._childs = self.scanner.scan( childs )
        else:
          self._childs = childs
        self.lastModified = currentTime
      result = self._childs
    finally:
      self._lock.release()
    return result


#----------------------------------------------------------------------------
def getId( name ):
  """
  Returns the name in lowercase.
  """
  return  name.lower()

#----------------------------------------------------------------------------
class BackwardCompatiblePattern( DictPattern ):
  """
  This class represents a file pattern. 
  
  The pattern is described with an expression like ``f|*.jpg``. 
  The first character (before the ``|``) indicates if the pattern recognizes files (``f``) or directories (``d``).
  This part is optional. The rest of the pattern is a simple regular expression that describes the file name. 
  It can contain ``*`` character to replace any character and ``#`` character to replace numbers.
  See the parent class :py:class:`brainvisa.data.patterns.DictPattern` for more details about the patterns.
  
  A method :py:meth:`match` enables to check if a :py:class:`DiskItem` file name matches this format pattern.
  """
  _msgBadPattern = '<em><code>%s</code></em> is not a valid pattern'

  def __init__( self, pattern ):
    i = pattern.find( '|' )
    if i >= 0:
      fileType = pattern[ :i ]
      # Check file type
      if fileType == 'fd':
        self.fileType = None
      elif fileType == 'f':
        self.fileType = File
      elif fileType == 'd':
        self.fileType = Directory
      else:
        raise ValueError( HTMLMessage(_t_(self._msgBadPattern) % pattern) )     
      p = pattern[ i+1: ]
    else:
      self.fileType = None
      p = pattern
    DictPattern.__init__( self, p )
    self.pattern = pattern
    
  def match( self, diskItem ):
    """
    Checks if the diskitem file name matches the pattern.
    
    :param diskItem: The :py:class:`DiskItem` whose file format is checked.
    :returns: a dictionary containing the value found in the diskItem file name for each named expression of the pattern. 
      The ``*`` character in the pattern is associated to a ``filename_variable`` key in the result dictionary.
      The ``#`` character in the pattern is associated to a ``name_serie`` key in the result dictionary.
      ``None`` is returned if the diskitem file name doesn't match the pattern.
    """
    # Check File / Directory / both
    if self.fileType is not None:
      if diskItem.__class__ is not DiskItem and \
         not isinstance( diskItem, self.fileType ):
        return None

    result = DictPattern.match( self, os.path.basename( diskItem.name ), diskItem )
    return result      

  def unmatch( self, diskItem, matchResult, force=False ):
    """
    The opposite of :py:meth:`match` method:  the matching string is found from a match result and a dictionary of attributes values. 
    
    :param matchResult: dictionary which associates a value to each named expression of the pattern.
    :param dict: dictionary which associates a value to each attribute name of the pattern.
    :param bool force: If True default values are set in the match result for ``filename_variable`` and ``name_serie`` attributes.
    :rtype: string
    :returns: The rebuilt matching string.
    """
    if matchResult is None: return None
    if force:
      matchResult.setdefault( 'filename_variable', '' )
      matchResult.setdefault( 'name_serie', [] )
    return DictPattern.unmatch( self, matchResult, diskItem )
    

#----------------------------------------------------------------------------
class BackwardCompatiblePatterns:
  """
  This class represents several file patterns. 
  
  Each pattern is a :py:class:`BackwardCompatiblePattern`.
  """
  _typeMsg = '<em><code>%s</code></em> is not a valid pattern list'
   
  def __init__( self, patterns ):
    """
    :param patterns: a string, a list of string, or a list of :py:class:`BackwardCompatiblePattern`. They are used to create the internal list of :py:class:`BackwardCompatiblePattern`.
    """
    # Build Pattern list in self.patterns
    if type( patterns ) is types.StringType:
      self.patterns = [ BackwardCompatiblePattern( patterns ) ]
    elif type( patterns ) in ( types.TupleType, types.ListType ):
      self.patterns = []
      for i in patterns:
        if isinstance( i, BackwardCompatiblePattern ):
          self.patterns.append( i )
        else:
          self.patterns.append( BackwardCompatiblePattern( i ) )
    else:
      raise TypeError( HTMLMessage(_t_(self._typeMsg) % str( patterns )) )
            
  def match( self, diskItem, returnPosition=0 ):
    """
    Checks if the diskitem matches one of the patterns of this :py:class:`BackwardCompatiblePatterns`.
    
    :param diskItem: The diskitem which files names should match the patterns.
    :param bool returnPosition: if True, the index of the matching pattern is returned as well as the match result.
    :returns: the match result or a tuple (match result, index) if ``returnPosition`` is True.
    """
    pos = 0
    for i in self.patterns:
      m = i.match( diskItem )
      if m:
        if returnPosition: return ( m, pos )
        return m
      pos += 1
    return None

  def unmatch( self, diskItem, matchResult, force = 0 ):
    """
    Returns the list of file names generated from a diskitem and its match result.
    """
    return [ p.unmatch( diskItem, matchResult, force )
             for p in self.patterns ]

  def fileOrDirectory( self ):
    """
    Checks if all the patterns match files or directories. If they all match the same type (file or directory), returns this type.
    """
    if self.patterns:
      result = self.patterns[0].fileType
      for p in self.patterns[ 1: ]:
        if p.fileType is not result: return None
      return result
    return None

  def __cmp__( self, other ):
    return self.patterns != other.patterns


#----------------------------------------------------------------------------
formats = {}
formatLists = {}

#----------------------------------------------------------------------------
class Format:
  """
  This class represents a data file format. It is used to define the format attribute of :py:class:`DiskItem` objects.
  It is also used to define new formats in brainvisa ontology files.
  
  :Attributes: 
  
  .. py:attribute:: name
  
    Name of the format. For example *GIS image*.
  
  .. py:attribute:: fileName
  
    Name of the python file that contains the definition of this format.
  
  .. py:attribute:: id
  
    Identifier associated to this format.
  
  .. py:attribute:: patterns
  
    :py:class:`BackwardCompatiblePatterns` describing the files patterns associated to this format. Example: ``f|*.ima, f|*.dim``.
  
  :Methods:
  
  """
  _msgError = 'error in <em>%s</em> format'
  
  def __init__( self, formatName, patterns, attributes=None, exclusive=None, 
                ignoreExclusive=0 ):
    """
    :param string formatName: name of the format. 
    :param patterns: string or :py:class:`BackwardCompatiblePatterns` describing the files patterns associated to this format.
    :param attributes: dictionary of attributes associated to this format. For example, an attribute *compressed = type_of_compression* is associated to the compressed files formats.
    :param bool exclusive: if True, a file should match only this pattern to match the format. Hardly ever used.
    :param bool ignoreExclusive: if True, this format will be ignored when exclusivity of a format is checked. Hardly ever used.
    """
    if type( formatName ) is not types.StringType:
      raise ValueError( HTMLMessage(_t_('<em><code>%s</code></em> is not a valid format name') % formatName) )
    
    tb=traceback.extract_stack(None, 3)
    self.fileName=tb[0][0]
    
    self.name = formatName
    self.id = getId( self.name )
    # Check patterns
    if isinstance( patterns, BackwardCompatiblePatterns ):
      self.patterns = patterns
    else:
      self.patterns = BackwardCompatiblePatterns( patterns )
    # Register self in formats
    f = formats.get( self.id )
    if f:
      if self.patterns != f.patterns:
        raise ValueError( HTMLMessage(_t_('format <em>%s</em> already exists whith a different pattern') % self.name) )
    else:
      formats[ self.id ] = self
    self._formatAttributes = attributes
    self._exclusive = exclusive
    self._ignoreExclusive = ignoreExclusive
    
  def __getstate__( self ):
    #raise cPickle.PicklingError
    return { 'name' : self.name }

  def __setstate__( self, state ):
    f = getFormat( state[ 'name' ] )
    self.__dict__.update( f.__dict__ )

  def match( self, item, returnPosition=0, ignoreExclusive=0 ):
    """
    Checks if the diskItem files match the format patterns.
    
    :param item: the :py:class:`DiskItem` whose format is checked.
    :param bool returnPosition: if True the position of the match is returned with the matched variables
    :param bool ignoreExclusive: if True, the exclusivity of the matched format will not be checked.
    :returns: a dictionary containing the matched variables of the patterns or a tuple containing this dictionary and the position of the match if returnPosition was True.
    """
    name = item.name
    if name[ -5: ] == '.minf':
      item.name = name[ : -5 ]
      result = self.patterns.match( item, returnPosition )
      item.name = name
    else:
      result = self.patterns.match( item, returnPosition )
    if self._exclusive and not ignoreExclusive:
      for f in getAllFormats():
        if f is not self and not f._ignoreExclusive and f.match( item, returnPosition, ignoreExclusive=1 ):
          return None
    return result

  def unmatch( self, diskItem, matchResult, force = 0 ):
    """
    Returns the list of filenames according to this format replacing the variables in the patterns with the value in matchResult. 
    """
    d, f = os.path.split( diskItem.name )
    if d:
      oldName = diskItem.name
      diskItem.name = f
      result = map( lambda x, d=d: os.path.join( d, x ), self.patterns.unmatch( diskItem, matchResult, force=force ) )
      diskItem.name = oldName
    else:
      result =  self.patterns.unmatch( diskItem, matchResult, force=force )
    return result

  def formatedName( self, item, matchResult ):
    """
    Returns the name of the diskItem that matches the format without format extension.
    """
    star = matchResult.get( 'filename_variable' )
    if star:
      if item.parent is None:
        d = os.path.dirname( item.name )
        if d:
          return os.path.join( d, star )
      return star
    return item.name
  
  def setFormat( self, item, ruleMatchingInfo = None ):
    """
    Sets this format to the diskItem.
    
    :param item: :py:class:`DiskItem` whose format will be set.
    :param ruleMatchingInfo: used in derived class.
    """
    item.format = self
  
  def group( self, groupedItem, matchedItem, position=0 ):
    if isinstance( matchedItem, Directory ) and not isinstance( groupedItem, Directory ):
      # If a directory is grouped, the final DiskItem is a Directory
      tmp = groupedItem
      groupedItem = matchedItem
      matchedItem = tmp
      groupedItem.__dict__.update( matchedItem.__dict__ )
    if matchedItem.fileName()[ -5: ] == '.minf':
      return groupedItem
    groupedItem._files[ position:position ] = matchedItem._files
    return groupedItem
  
  def fileOrDirectory( self ):
    """
    Returns the class of diskItem that this format matches : :py:class:`File` or :py:class:`Directory`.
    """
    return self.patterns.fileOrDirectory()
  
  def __str__( self ):
    return self.name
  
  def __repr__( self ):
    return  repr( self.name )

  def setAttributes( self, item, writeOnly=0 ):
    """
    Adds the attributes of this format to the diskItem other attributes
    """
    attrs = self._formatAttributes
    if attrs is not None:
      if type( attrs ) is types.DictType:
        item._updateOther( attrs )
      elif callable( attrs ):
        item._updateOther( attrs( item, writeOnly=writeOnly ) )
      else:
        raise ValueError( HTMLMessage(_t_('Invalid attributes: <em>%s</em>') % htmlEscape( str(attrs) )) )

  def postProcessing( self, item ):
    """Virtual method. Overriden in derived class.
    """
    pass

  def getPatterns( self ):
    """Returns a :py:class:`BackwardCompatiblePatterns` representing the files patterns for this format.
    """
    return self.patterns


#----------------------------------------------------------------------------
class MinfFormat( Format ):
  """
  Base class for the file formats that uses the minf format.
  """
  pass
    
#----------------------------------------------------------------------------
class FormatSeries( Format ):
  """
  This class represents the format of a serie of diskItems with the same format. For example, a serie of images in Analyse format.
  Such an object can be created using the function :py:func:`changeToFormatSeries`.
  
  :Attributes:
  
  .. py:attribute:: baseFormat
  
    The format of each element of the serie.
  
  :Methods:
  
  """
  def __init__( self, baseFormat, formatName=None, attributes=None ):
    baseFormat = getFormat( baseFormat )
    if isinstance( baseFormat, FormatSeries ):
      raise ValueError( HTMLMessage(_t_('Impossible to build a format series of <em>%s</em> which is already a format series' ) % ( baseFormat.name )) )
    if formatName is None:
      formatName = 'Series of ' + baseFormat.name
    if type( formatName ) is not types.StringType:
      raise ValueError( HTMLMessage(_t_('<em><code>%s</code></em> is not a valid format name') % formatName) )
    if attributes is None:
      attributes = baseFormat._formatAttributes
    
    tb=traceback.extract_stack(None, 3)
    self.fileName=tb[0][0]
    self.name = formatName
    self.id = getId( self.name )
    self.baseFormat = baseFormat
    registerFormat( self )
    self._formatAttributes = attributes
    self._ignoreExclusive = baseFormat._ignoreExclusive

  def match( self, *args, **kwargs ):
    """
    Calls the :py:meth:`Format.match` method of the base format.
    """
    return self.baseFormat.match( *args, **kwargs )

  def unmatch( self, *args, **kwargs ):
    """
    Calls the :py:meth:`Format.unmatch` method of the base format.
    """
    return self.baseFormat.unmatch( *args, **kwargs )

  def formatedName( self, item, matchResult ):
    """
    Calls the :py:meth:`Format.formatedName` method of the base format.
    """
    return self.baseFormat.formatedName( item, matchResult )
    
  def setFormat( self, item, ruleMatchingInfo=None ):
    """
    Applies this format to the diskitem.
    """
    item.format = self
    if ruleMatchingInfo is not None:
      rule, matchRule = ruleMatchingInfo
      ns = matchRule.get( 'name_serie' )
      matchRule[ 'name_serie' ] = '#'
      item.name = rule.pattern.unmatch( item, matchRule )
      matchRule[ 'name_serie' ] = ns
      item._files = self.unmatch( item, { 'filename_variable': item.name, } )
      if ns:
        item._getLocal( 'name_serie' ).append( ns )
      #if not isinstance( ns, basestring ):
        #ns.append( matchRule.get( 'name_serie' ) )

  def group( self, groupedItem, matchedItem, position=0, matchRule=None ):
    """
    Groups files which are associated to the same diskitem.
    """
    if isinstance( matchedItem, Directory ) and not isinstance( groupedItem, Directory ):
      # If a directory is grouped, the final DiskItem is a Directory
      tmp = groupedItem
      groupedItem = matchedItem
      matchedItem = tmp
      groupedItem.__dict__.update( matchedItem.__dict__ )
    if matchedItem.fileName()[ -5: ] == '.minf':
      return groupedItem
    if matchRule:
      ns = groupedItem._getLocal( 'name_serie' )
      if not isinstance( ns, basestring ):
        ns.append( matchRule.get( 'name_serie' ) )
    return groupedItem
    
  def fileOrDirectory( self ):
    """
    Calls the :py:meth:`Format.fileOrDirectory` method of the base format.
    """
    return self.baseFormat.fileOrDirectory()

  def postProcessing( self, item ):
    """
    Sorts and removes doubloons in the list of numbers in the ``name_serie`` attribute of the item.
    """
    name_serie = item.get( 'name_serie' )
    if len( name_serie ) > 1:
      # Sort name_serie by numeric order
      numbers = [ (long(i), i) for i in name_serie ]
      numbers.sort()
      name_serie = [ i[ 1 ] for i in numbers ]
      # Remove identical entries
      i = 1
      while i < len( name_serie ):
        if name_serie[ i ] == name_serie[ i-1 ]:
          del name_serie[ i ]
        else:
          i += 1
      item._setLocal( 'name_serie', name_serie )

  def getPatterns( self ):
    """
    Calls the :py:meth:`Format.getPatterns` method of the base format.
    """
    return self.baseFormat.patterns
  

#----------------------------------------------------------------------------
def changeToFormatSeries( format ):
  """
  Gets the suited format for a serie of item in the given format.
  """
  if isinstance( format, FormatSeries ):
    return format
  global formats
  result = formats.get( getId( 'Series of ' + format.name ), None )
  if not result:
    result = FormatSeries( format )
  return result


#----------------------------------------------------------------------------
def registerFormat( format ):
  """
  Stores the format in a global list of formats.
  """
  global formats
  f = formats.get( format.id )
  if f:
    raise ValueError( HTMLMessage(_t_('format <em>%s</em> already exists') % format.name) )
  else:
    formats[ format.id ] = format


#----------------------------------------------------------------------------
def getFormat( item, default=Undefined ):
  """
  Gets the format whose id is the given item.
  """
  if isinstance( item, Format ): return item
  elif isinstance( item, basestring ):
    if item == 'Graph': item = 'Graph and data'
    result = formats.get( getId( item ) )
    if result: return result
    if item.startswith( 'Series of ' ):
      result = changeToFormatSeries( getFormat( item[ 10: ] ) )
      if result: return result
  if default is Undefined:
    raise ValueError( HTMLMessage(_t_('<em><code>%s</code></em> is not a valid format') % str( item )) )
  return default


#----------------------------------------------------------------------------
class NamedFormatList( UserList ):
  """
  This class represents a list of formats which have a name.
  This object can be used as a list.
  
  .. py:attribute:: name
    
    Name of the list of formats.
  
  .. py:attribute:: data
    
    List of formats.
  
  .. py:attribute:: fileName
    
    Name of the file where this list of formats is defined.
    
  """
  def __init__( self, name, data ):
      tb=traceback.extract_stack(None, 3)
      self.fileName=tb[0][0]
      self.name = name
      self.data = list(data)

  
  def __str__( self ):
      return self.name

  def __repr__( self ):
      return repr( self.name )

  def __getstate__( self ):
    return ( self.name, self.data )
  
  def __setstate__( self, state ):
    self.name, self.data = state

  def __add__(self, other):
    if isinstance(other, UserList):
      return self.data + other.data
    elif isinstance(other, type(self.data)):
        return self.data + other
    else:
        return self.data + list(other)

  def __radd__( self, other ):
    if isinstance(other, UserList):
      return other.data + self.data
    elif isinstance(other, type(self.data)):
      return other + self.data
    else:
      return list(other) + self.data
      
  def __mul__(self, n):
      return self.data*n
  __rmul__ = __mul__


#----------------------------------------------------------------------------
def createFormatList( listName, formats=[] ):
  """
  Creates a new :py:class:`NamedFormatList` and stores it in a global list of formats lists.
  
  :param string listName: the name of the list
  :param list formats: the list of formats ids (string)
  :returns: the new :py:class:`NamedFormatList` containing a list of :py:class:`Format` objects.
  """
  global formatLists
  key = getId( listName )
  if formatLists.has_key( key ):
    raise KeyError( listName )
  result = NamedFormatList( listName, getFormats( formats ) )
  formatLists[ key ] = result
  return result


#----------------------------------------------------------------------------
def getFormats( formats ):
  """
  Returns a list of :py:class:`Format` or a :py:class:`NamedFormatList` corresponding to the given parameter.
  The parameter can be a string or a list of string matching format names.
  """
  if formats is None: return None
  global formatLists
  if type( formats ) in types.StringTypes:
    key = getId( formats )
    result = formatLists.get( key )
    if result is None:
      result = [ getFormat( formats ) ]
  elif isinstance( formats, NamedFormatList ):
    return formats
  elif isinstance( formats, Format ):
    result = [formats]
  else:
    if [i for i in formats if not isinstance( i, Format )]:
      result = [getFormat(i) for i in formats]
    else:
      result = formats
  return result


#----------------------------------------------------------------------------
def getAllFormats():
  """
  Returns the list of all available formats.
  """
  global formats
  return formats.values()

directoryFormat = Format( 'Directory', 'd|*', ignoreExclusive=1 )
fileFormat = Format( 'File', 'f|*', ignoreExclusive=1 )


#----------------------------------------------------------------------------
diskItemTypes = {}


#----------------------------------------------------------------------------
class DiskItemType:
  """
  This class represents a data type. It is used to define the type attribute of :py:class:`DiskItem` objects.
  
  The types are defined hierarchically, that's why a type can have a parent type 
  and a method :py:meth:`DiskItemType.isA` enables to request if a type is derived from another type.
  
  :Attributes: 
  
  .. py:attribute:: name
    
    Name of the Type. For example *T1 MRI*.
  
  .. py:attribute:: fileName
  
    Name of the python file that contains the definition of this type.
  
  .. py:attribute:: id
  
    Identifier associated to this type.
  
  .. py:attribute:: parent
  
    As types are defined hierarchically, a type can have a parent type.
  
  :Methods:

  """
  def __init__( self, typeName, parent = None, attributes=None ):
    """
    :param string typeName: name of the type
    :param parent: can be the name of the parent. If it already registered, it will be found from its name.
    :param attributes: dictionary containing attributes associated to this type. 
    """
    # Check name
    if type( typeName ) is not types.StringType:
      raise ValueError( _t_('a type name must be a string') )
    
    tb=traceback.extract_stack(None, 3)
    self.fileName=tb[0][0]
    self.name = typeName
    self.id = getId( typeName )
    if parent is None: self.parent = None
    else: self.parent = getDiskItemType( parent )
    other = diskItemTypes.get( self.id )
    if other:
      if not sameContent( self, other ):
        raise ValueError( HTMLMessage(_t_( 'invalid redefinition for type <em>%s</em>') % self.name) )
    else:
      diskItemTypes[ self.id ] = self
    if attributes is None:
      if self.parent is not None:
        self._typeAttributes = self.parent._typeAttributes
      else:
        self._typeAttributes = None
    else:
      self._typeAttributes = attributes
  
  def __getstate__( self ):
    #raise cPickle.PicklingError
    return { 'diskItemType' : self.name }

  def __setstate__( self, state ):
    dit = getDiskItemType( state[ 'diskItemType' ] )
    self.__dict__.update( dit.__dict__ )

  def setType( self, item, matchResult, formatPosition ):
    """
    Sets this type to the given item.
    """
    item.type = self

  def isA( self, diskItemType ):
    """
    Returns True if the given diskItem type is a parent (direct or indirect) of the current diskItem.
    """
    diskItemType = getDiskItemType( diskItemType )
    if diskItemType is self: return 1
    if self.parent is None: return 0
    return self.parent.isA( diskItemType )

  def parents( self ):
    """
    Returns the list of parents of this diskItem, that is to say its parent and its parent's parents.
    """
    if self.parent: return [ self.parent ] + self.parent.parents()
    return []

  def __str__( self ):
    return self.name

  def __repr__( self ):
    return '<' + str( self ) + '>'

  def setAttributes( self, item, writeOnly=0 ):
    """
    Add this type attributes to the given diskItem minf attributes.
    """
    attrs = self._typeAttributes
    if attrs is not None:
      if type( attrs ) is types.DictType:
        item.updateMinf( attrs )
      elif callable( attrs ):
        item.updateMinf( attrs( item, writeOnly=writeOnly ) )
      else:
        raise ValueError( HTMLMessage(_t_('Invalid attributes: <em>%s</em>') % htmlEscape( str(attrs) )) )


#----------------------------------------------------------------------------
def getDiskItemType( item ):
  """
  Gets the :py:class:`DiskItemType` whose id is the given item.
  """
  if isinstance( item, DiskItemType ): return item
  elif type( item ) is types.StringType or type( item ) is types.UnicodeType:
    result = diskItemTypes.get( getId( item ) )
    if result: return result
  raise ValueError( HTMLMessage(_t_('<em><code>%s</code></em> is not a valid file or directory type') % str( item )) )


#----------------------------------------------------------------------------
def getAllDiskItemTypes():
  """
  Gets all registered :py:class:`DiskItemType`.
  """
  return diskItemTypes.values()


#----------------------------------------------------------------------------
def isSameDiskItemType( base, ref ):
  """
  Returns True if ref is a parent of base.
  """
  if base: return base.isA( ref )
  else: return ref is None



#----------------------------------------------------------------------------
class FileType( DiskItemType ):
  """
  This class represents a type for a DiskItem associated to files (a :py:class:`File`).
  It is used to define new data types in brainvisa ontology files.
  
  It can store file formats associated to this type.
  
  """
  def __init__( self, typeName, parent = None, formats = None, minfAttributes = None ):
    """
    :param string typeName: name of the type
    :param formats: list of file formats associated to this type of data.
    :param minfAttributes: dictionary containing attributes associated to this type.
    """
    # Check formats
    if formats:
      self.formats = getFormats( formats )
    elif parent:
      parent = getDiskItemType( parent )
      self.formats = parent.formats
    else:
      self.formats = None
    # Register type
    DiskItemType.__init__( self, typeName, parent, minfAttributes )

  def sameContent( self, other ):
    """
    Returns True if the two file types are instances of the same class and have the same list of formats and the same parent.
    
    The formats and parents are compared using the function :py:func:`sameContent`.
    """
    return isinstance( other, FileType ) and \
           self.__class__ is  other.__class__ and \
           sameContent( self.formats, other.formats ) and \
           sameContent( self.parent, other.parent )

#----------------------------------------------------------------------------
def expand_name_serie( text, number ):
  """
  Replaces ``#`` character in *text* by *number*.
  """
  l = text.split( '#' )
  if len( l ) == 2:
    return l[0] + number + l[1]
  return text

#----------------------------------------------------------------------------
class TemporaryDirectory( Directory ):
  """
  This class represents a temporary directory. 
  
  It will be deleted when Brainvisa closes.
  
  The deletion of temporary files and directories is managed by a :py:class:`brainvisa.data.temporary.TemporaryFileManager`.
  """
  def __init__( self, name, parent ):
    self._isTemporary = 1
    if parent:
      fullPath = os.path.join( parent.fullPath(), name )
    else:
      fullPath = name
    if not os.path.isdir( fullPath ):
      try:
        os.mkdir( fullPath, 0770 )
      except OSError, e:
        if not e.errno == errno.EEXIST:
          # filter out 'File exists' exception, if the same dir has been created
          # concurrently by another instance of BrainVisa or another thread
          raise
    Directory.__init__( self, name, parent )


#----------------------------------------------------------------------------
class TemporaryDiskItem( File ):
  """
  This class represents a temporary diskitem.
  
  The associated files are automatically deleted when the corresponding object is no more used and so garbage collected.
  
  The deletion of temporary files and directories is managed by a :py:class:`brainvisa.data.temporary.TemporaryFileManager`.
  """
  def __init__( self, name, parent ):
    File.__init__( self, name, parent )
    self._isTemporary = 1
    
  def __del__( self ):
    if Application().configuration.brainvisa.removeTemporary:
      toDelete = self.fullPaths()
      toDelete.append( toDelete[ 0 ] + '.minf' )
      #print 'deleting temp DI:', toDelete
      for f in toDelete:
        n = 0
        while 1:
          try:
            temporary.manager.removePath( f )
            break
          except:
            if n < 100:
              n += 1
              time.sleep( 0.01 )
              #print 'can\' delete', f, 'yet. waiting'
              #sys.stdout.flush()
            else:
              #print 'exception while removing', f
              showException( beforeError=_t_('temorary file <em>%s</em> not '
                                             'deleted<br>') % f, gui=0 )
              # giving up, let it for later
              temporary.manager.registerPath( f )
              print 'continuing after failed rm'
              sys.stdout.flush()
              break

  def clear( self ):
    """
    Removes all files associated to this diskItem.
    """
    for f in self.fullPaths():
      try:
        temporary.manager.removePath( f )
        temporary.manager.registerPath( f )
      except:
        showException( beforeError=_t_('temorary file <em>%s</em> not deleted<br>') % f,
          gui=0 )


#----------------------------------------------------------------------------
globalTmpDir = None


#----------------------------------------------------------------------------
def getTemporary( format, diskItemType = None, parent = None, name = None ):
  """
  Creates a new temporary diskitem. It will be automatically deleted when there is no more references on it or when Brainvisa closes.
  
  :param format: format of the diskitem. 
    If the format correspond to a directory, a :py:class:`TemporaryDirectory` will be created, else a :py:class:`TemporaryDiskItem` will be created.
  :param diskItemType: type of the diskItem. 
  :param parent: parent diskitem: directory which contains the diskItem. 
    By default it will be Brainvisa global temporary directory whose path can be choosen in the options.
    The global temporary directory diskitem is defined in the global variable :py:data:`globalTmpDir`.
  :param name: filename for the diskitem. By default, a new name is generated by the :py:class:`brainvisa.data.temporary.TemporaryFileManager`.
  :returns: a :py:class:`TemporaryDirectory` or a :py:class:`TemporaryDiskItem`.
  """
  global globalTmpDir

  format = getFormat( format )
  if diskItemType is not None: diskItemType = getDiskItemType( diskItemType )
  if parent is None:
    if globalTmpDir is None:
      globalTmpDir = TemporaryDirectory( neuroConfig.temporaryDirectory, None )
    parent = globalTmpDir

  if name is None:
    name = temporary.manager.newFileName( directory=parent.fullPath() )
  if format.fileOrDirectory() is Directory:
    item = TemporaryDirectory( name, parent )
  else:
    item = TemporaryDiskItem( name, parent )
  item._files = format.unmatch( item, { 'filename_variable': name, 'name_serie': None }, 1 )
  item.format = format
  item.type = diskItemType
  toDelete = item.fullPaths()
  toDelete.append( toDelete[ 0 ] + '.minf' )
  for f in toDelete:
    temporary.manager.registerPath( f )
  return item


#----------------------------------------------------------------------------
_uuid_to_DiskItem = WeakValueDictionary()

#----------------------------------------------------------------------------
def getDataFromUuid( uuid ):
  """
  Gets a :py:class:`DiskItem` from its uuid.
  """
  if not isinstance( uuid, Uuid ):
    uuid = Uuid( uuid )
  return _uuid_to_DiskItem.get( uuid )

#----------------------------------------------------------------------------
class HierarchyDirectoryType( FileType ):
  """
  This type represents a directory which represents data in Brainvisa ontology.
  It is associated to the format :py:data:`directoryFormat`.
  """
  def __init__( self, typeName, parent=None, **kwargs ):
    FileType.__init__( self, typeName, parent, directoryFormat, **kwargs )
  
#----------------------------------------------------------------------------
typesLastModification = 0
# mef is global to handle multiple call to readTypes since allready read
# file types are stored in it to prevent multiple loads which cause troubles.
class TypesMEF( MultipleExecfile ):
  """
  This class enables to read Brainvisa ontology files which contain the definition of formats and types.
  
  A global instance of this object :py:data:`mef` is used to load types and formats at Brainvisa startup.
  
  The Brainvisa types files will define new formats with :py:class:`Format`, new types with :py:class:`FileType`
  and new format lists with :py:func:`createFormatList`. 
  
  With the :py:attr:`brainvisa.multipleExecfile.MultipleExecfile.localDict`, callback methods are associated to the functions that will be called in the types files:
  :py:meth:`create_format`, :py:meth:`create_format_list`, :py:meth:`create_type`.
  
  """
  def __init__( self ):
    super( TypesMEF, self ).__init__()
    self.localDict[ 'Format' ] = self.create_format
    self.localDict[ 'createFormatList' ] = self.create_format_list
    self.localDict[ 'changeToFormatSeries' ] = self.create_format_serie
    self.localDict[ 'FileType' ] = self.create_type
    self.localDict[ 'HierarchyDirectoryType' ] = self.create_hie_dir_type
  
  
  def create_format( self, *args, **kwargs ):
    """
    This method is called when a new format is created in one of the executed files.
    The *toolbox*, *module*  and fileName attributes of the new :py:class:`Format` are set.
    
    :param args: The arguments will be passed to the constructor of :py:class:`Format`.
    :returns: The new format.
    """
    format = Format( *args, **kwargs )
    toolbox, module = self.currentToolbox()
    format.toolbox = toolbox
    format.module = module
    format.fileName=self.localDict["__name__"]
    return format
  
  def create_format_list( self, *args, **kwargs ):
    """
    This method is called when a new formats list is created in one of the executed files.
    The *toolbox*, *module*  and fileName attributes of the new :py:class:`NamedFormatList` are set.
    
    :param args: The arguments will be passed to the function :py:func:`createFormatList`.
    :returns: The new formats list.
    """
    format_list = createFormatList( *args, **kwargs )
    toolbox, module = self.currentToolbox()
    format_list.toolbox = toolbox
    format_list.module = module
    format_list.fileName=self.localDict["__name__"]
    return format_list
    
  def create_format_serie( self, *args, **kwargs ):
    """
    This method is called when a new formats list is created in one of the executed files.
    The *toolbox*, *module*  and fileName attributes of the new :py:class:`FormatSeries` are set.
    
    :param args: The arguments will be passed to the function :py:func:`changeToFormatSeries`.
    :returns: The new format series.
    """
    format_serie = changeToFormatSeries( *args, **kwargs )
    toolbox, module = self.currentToolbox()
    format_serie.toolbox = toolbox
    format_serie.module = module
    format_serie.fileName=self.localDict["__name__"]
    return format_serie

  def create_type( self, *args, **kwargs ):
    """
    This method is called when a new type is created in one of the executed files.
    The *toolbox*, *module*  and fileName attributes of the new :py:class:`FileType` are set.
    
    :param args: The arguments will be passed to the constructor of :py:class:`FileType`.
    :returns: The new type.
    """
    type = FileType( *args, **kwargs )
    toolbox, module = self.currentToolbox()
    type.toolbox = toolbox
    type.module = module
    type.fileName=self.localDict["__name__"]
    return type
  
  def create_hie_dir_type( self, *args, **kwargs ):
    """
    This method is called when a new type is created in one of the executed files.
    The *toolbox*, *module*  and fileName attributes of the new :py:class:`HierarchyDirectoryType` are set.
    
    :param args: The arguments will be passed to the constructor of :py:class:`HierarchyDirectoryType`.
    :returns: The new type.
    """
    hdtype=HierarchyDirectoryType(*args, **kwargs)
    toolbox, module = self.currentToolbox()
    hdtype.toolbox = toolbox
    hdtype.module = module
    hdtype.fileName = self.localDict["__name__"]
    return hdtype
  
  def currentToolbox( self ):
    """
    Finds the name of the toolbox and the module path of the last executed file.
    Returns a tuple *(toolbox, module)*.
    """
    file = self.localDict[ '__name__' ]
    toolbox = None
    module = None
    if file.startswith( neuroConfig.mainPath ):
      l = split_path( file[ len( neuroConfig.mainPath ) + 1: ] )
      if l and l[0] == 'toolboxes':
        if len( l ) >= 4:
          toolbox = l[ 1 ]
          module = '.'.join( l[ 2:] )
          if module.endswith( '.py' ):
            module = module[ :-3 ]
      elif l and l[0] == 'types':
        toolbox = 'axon'
        module = '.'.join( l )
        if module.endswith( '.py' ):
          module = module[ :-3 ]
    else:
      module = file
    return ( toolbox, module )
  
mef = TypesMEF()
mef.fileExtensions.append( '.py' )
  
  
def readTypes():
  """
  This function loads types and formats by reading Brainvisa ontology types files.
  """
  global typesLastModification
  global mef
  mef.includePath.update(neuroConfig.typesPath)
  try:
    files = shelltools.filesFromShPatterns( *[os.path.join( path, '*.py' ) for path in neuroConfig.typesPath] )
    files.sort()
    exc=mef.execute( continue_on_error=True, *files )
    if exc:
      for e in exc:
        try:
          raise e
        except:
          showException(beforeError="Error while reading types file: ")
    typesLastModification = max( (os.stat(f).st_mtime for f in mef.executedFiles()) )
  except:
    showException()


def reloadTypes():
  """
  This function reinitializes the global variables used to store the types and formats before calling :py:func:`readTypes`.
  """
  global formats
  global formatLists
  global diskItemTypes
  global typesLastModification
  global mef
  global directoryFormat
  global fileFormat
  
  formats={}
  formatLists={}
  diskItemTypes={}
  typesLastModification=0
  mef = TypesMEF()
  mef.fileExtensions.append( '.py' )
  directoryFormat = Format( 'Directory', 'd|*', ignoreExclusive=1 )
  fileFormat = Format( 'File', 'f|*', ignoreExclusive=1 )
  
  readTypes()
  
  
#----------------------------------------------------------------------------
try:
  from soma import aims
  
  # Fix libxml2 multithreaded application issue by initializing parser from the main thread
  aims.xmlInitParser()
  
  _finder = aims.Finder()
  # don't resolve symlinks if file browser to be consistent with
  # all DiskItem namings
  try:
    aims.setQtResolveSymlinks( False )
  except:
    pass
except:
  _finder = None


#----------------------------------------------------------------------------
def aimsFileInfo( fileName ):
  """
  Reads the header of the file *fileName* and returns its attributes as a dictionary.
  """
  from neuroProcesses import defaultContext
  from neuroProcessesGUI import mainThreadActions
  global _finder
  result = {}
  if fileName.endswith( '.ima.gz' ) or fileName.endswith( '.dim.gz' ) or fileName.endswith( '.ima.Z' ) or fileName.endswith( '.dim.Z' ):
    context = defaultContext()
    tmp = context.temporary( 'GIS image' )
    context.runProcess( 'uncompressGIS', fileName, tmp, False )
    fileName = tmp.fullPath()
  try:
    try:
      import numpy
      nan = numpy.nan
    except ImportError:
      nan = None
    if _finder is not None:
      finder = aims.Finder()
      if type( fileName ) is unicode:
        # convert to str
        import codecs
        fileName = codecs.getencoder( 'utf8' )( fileName )[0]
      # Finder is not thread-safe (yet)
      if mainThreadActions().call( finder.check, fileName ):
        result = eval( str(finder.header() ), locals())
    else:
      if neuroConfig.platform == 'windows':
        f=os.popen( 'AimsFileInfo -i "' + fileName + '"', 'r' )
      else:
        f=os.popen( 'AimsFileInfo -i "' + fileName + '" 2> /dev/null', 'r' )
      s = f.readline()
      while s and s != 'attributes = {\n': s = f.readline()
      s = s[13:-1] + f.read()
      result = eval( s , locals())
      f.close()
  except:
    pass
  return result
