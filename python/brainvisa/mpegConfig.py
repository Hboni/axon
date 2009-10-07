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

from neuroConfig import *


def findEncoders():
  enc = []
  if findInPath( 'mencoder' ):
    enc.append( 'mencoder' )
  if findInPath( 'transcode' ):
    enc.append( 'transcode' )
  if findInPath( 'recmpeg' ):
    enc.append( 'recmpeg' )
  if findInPath( 'ffmpeg' ):
    enc.append( 'ffmpeg' )
  return enc


def findMpegFormats():
  form = [ 'MPEG film' ]
  enc = findEncoders()
  if 'transcode' in enc or 'mencoder' in enc or 'ffmpeg' in enc:
    form.append( 'AVI film' )
  if 'mencoder' in enc or 'ffmpeg' in enc:
    form.append( 'QuickTime film' )
  return form


def findCodec( encoder ):
  if encoder == 'recmpeg':
    return [ 'mpeg1', 'mpeg4' ]
  elif encoder == 'transcode':
    return [ 'divx5', 'ffmpeg4', 'af6', 'divx4',
             'divx4raw', 'fame', 'im',
             'mjpeg', 'im', 'net', 'opendivx',
             'pcm', 'raw', 'toolame', 'xvid',
             'xvidcvs', 'xvidraw' ]
  elif encoder == 'mencoder':
    return [ 'mpeg4', 'mjpeg', 'ljpeg', 'h263', 'h263p', 'msmpeg4',
             'msmpeg4v2', 'wmv1', 'wmv2', 'rv10', 'mpeg1video',
             'mpeg2video', 'huffyuv', 'asv1', 'asv2', 'ffv1' ]
    #( h, f, g ) = os.popen3( 'mencoder -ovc help' )
    #l = f.readlines()
    #f.close()
    #r = 0
    #import re
    #reg = re.compile( '^\s+([^ ]+)\s+' )
    #cx = []
    #for x in l:
      #if x == 'Available codecs:\n':
        #r = 1
      #elif r == 1:
        #m = reg.match( x )
        #if m:
          #cx.append( m.group(1) )
    #return cx
  elif encoder == 'ffmpeg':
    #return [ 'asv1', 'asv2', 'dvvideo', 'ffv1', 'h263', 'huffyuv', 'h263p',
    #         'ljpeg', 'mjpeg', 'mpeg4', 'mpeg1video', 'mpeg2video', 'msmpeg4',
    #         'msmpeg4v1', 'msmpeg4v2', 'rv10', 'rv20', 'wmv1', 'wmv2', 'wmv3' ]
    try:
      # Valid only since Python 2.4
      import subprocess
      out, err = subprocess.Popen( ( 'ffmpeg', '-formats' ), 
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE ).communicate()
    except ImportError:
      # Work with earlier Python version but generates the following error at exit:
      # Exception exceptions.TypeError: TypeError("'NoneType' object is not callable",) in <bound method Popen3.__del__ of <popen2.Popen3 instance at 0xb7303c2c>> ignored
      stdin, stdout, stderr = os.popen3( 'ffmpeg -formats' )
      stdin.close()
      err = stderr.read()
      out = stdout.read()
      stdout.close()
      stderr.close()
      del stdout, stderr, stdin
    
    codecs = []
    lines = out.split( '\n' )
    codecsFound = False
    while lines:
      if lines.pop( 0 ) == 'Codecs:':
        codecsFound = True
        break
    if codecsFound:
      while lines:
        l = lines.pop( 0 )
        if not l: break
        codecs.append( l.split()[ -1 ] )
    return codecs
  return []


def findCodecs():
  c = {}
  for x in findEncoders():
    c[ x ] = findCodec( x )
  return c


encoders = findEncoders()
mpegFormats = findMpegFormats()
codecs = findCodecs()
