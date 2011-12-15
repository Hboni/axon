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
import sys, new, os
import registration
import neuroConfig
import neuroLog
import neuroException
import neuroData
from brainvisa.validation import ValidationError
from soma.qtgui.api import QtThreadCall
import distutils.spawn
import weakref, types, threading
import atexit
import copy
import backwardCompatibleQt as qt
try:
  import anatomist
  anatomist.setDefaultImplementation( neuroConfig.anatomistImplementation )
  exec("import "+anatomist.getDefaultImplementationModuleName()+" as anatomistModule")
  anatomistImport=True
except Exception, e:
  print e
  anatomistImport=False
"""
This module enables to generate an implementation of anatomist api specialized for brainvisa.
It can use either socket or direct(sip bindings) implementation.

To use this implementation, load the module using :

anatomistModule = anatomistapi.bvimpl.loadAnatomistModule(anatomistapi.api.SOCKET) # or DIRECT)
a=anatomistModule.Anatomist()
...
It can be used only in brainvisa application because it uses module that are loaded in brainvisa.

Returned module contains an Anatomist class which inherits from choosen Anatomist class implementation, and is a thread safe singleton.
Specifities added for brainvisa are :
  - loading referentials and transformation on loading an object using brainvisa database informations.
  - writing messages in brainvisa log file.
"""

def validation():
  if not anatomistImport:
    raise ValidationError('Cannot find anatomist module')
  elif distutils.spawn.find_executable('anatomist') is None:
    raise ValidationError( 'Cannot find Anatomist executable' )

if anatomistImport:
  # dynamic class Anatomist inherits from one implementation of anatomist api
  class Anatomist(anatomistModule.Anatomist):
    defaultRefType="WeakShared"
    def __singleton_init__(self, *args, **kwargs):
      anatomistParameters=[]
      for a in args:
        anatomistParameters.append(a)
      try:
        if neuroConfig.anatomistImplementation == 'socket':
          anatomistModule.Anatomist.anatomistExecutable = \
            eval("neuroConfig.anatomistExecutable")
        anatomistLog=neuroConfig.mainLog.subLog()
        communicationLog=anatomistLog.subTextLog()
        # log file for writing traces from this class
        self.communicationLogFile=open( communicationLog.fileName,'w' )
        # log for writing traces from anatomist process
        self.outputLog=anatomistLog.subTextLog()
        # add a trace in brainvisa main log
        neuroConfig.mainLog.append( 'Anatomist ', html=self.outputLog,
                children = [ neuroLog.LogFile.Item('Communications', html = communicationLog ) ],
                icon='anaIcon_small.png' )
        if neuroConfig.anatomistImplementation == 'socket':
          anatomistParameters+=[ '--cout', self.outputLog.fileName, '--cerr', self.outputLog.fileName ]
      except:
        neuroException.showException()
        self.communicationLogFile=None
        self.outputLog=None
      if neuroConfig.userProfile is not None:
        anatomistParameters += [ '-u', neuroConfig.userProfile ]
      mainThread=QtThreadCall()
      args = anatomistParameters
      super( Anatomist, self ).__singleton_init__( *args, **kwargs )
      if neuroConfig.anatomistImplementation != 'socket':
        a = anatomistModule.Anatomist()
        if neuroConfig.openMainWindow :
          # Anatomist can be closed directly if brainvisa main window is not available (this is used when brainvisa is 
          # launched as backgroud scrip and launches anatomist
          a.getControlWindow().enableClose( False )
      try:
        mainThread.push( neuroConfig.qtApplication.connect,
          neuroConfig.qtApplication,qt.SIGNAL( 'aboutToQuit ()' ),self.close )
      except:
        atexit.register(self.close)
                                     

    ###############################################################################
    # Methods redefined to use Brainvisa log system.
    def log(self, message ):
      """
      This method is redefined to print logs in brainvisa log system.
      """
      self.lock.acquire()
      if self.communicationLogFile is None:
          return
      self.communicationLogFile.write( message+"<BR>" )
      self.communicationLogFile.flush()
      self.lock.release()

    def logCommand(self, command, **kwargs):
      if self.communicationLogFile is not None:
        if isinstance(command, str):
          logcmd = "<B>-->" + command + "</B><UL>"
          for ( name, value ) in kwargs.items():
              if value is not None:
                  logcmd += "<LI>" + name + " = " + str( value )
          logcmd += "</UL>"
          self.log( logcmd )
        else: # if the command is not a string it should an object Command from anatomist bindings
          cmdDesc=command.write()
          logcmd = "<B>-->" + command.name() + "</B><UL>"
          for ( name, value ) in cmdDesc.items():
            if value is not None:
              logcmd += "<LI>" + name + " = " + str( value )
          logcmd += "</UL>"
          self.log( logcmd )

    def logEvent(self, event, params):
      """
      Log an event received from anatomist application.

      @type event: string
      @param event: event's name
      @type params: string
      @param params: event's parameters
      """
      self.log("<B> &lt;-- " + event + "</B> : " + params)

    ###############################################################################
    # Methods redefined to use Brainvisa transformation manager (with database informations)
    # When a database object is loaded, associated referential and transformations are loaded

    def loadObject(self, fileref, objectName=None, restrict_object_types=None, forceReload=False, loadReferential=True, duplicate=False, hidden=False):
      """
      Loads an object from a file (volume, mesh, graph, texture...)

      @type fileref: string or Diskitem
      @param fileref: name of the file that contains the object to load or a Diskitem representing this file.
      @type objectName: string
      @param objectName: object's name
      @type restrict_object_types: dictionary
      @param restrict_object_types: object -> accpepted types list. Ex: {'Volume' : ['S16', 'FLOAT']}
      @type forceReload: boolean
      @param forceReload: if True the object will be loaded even if it is already loaded in Anatomist. Otherwise, the already loaded one is returned.
      @type loadReferential: boolean
      @param loadReferential: if True, use brainvisa database informations to search referential and transformations associated to this object and load them.

      @rtype: AObject
      @return: the loaded object
      """
      if hasattr( fileref, "fullPath" ): # is it a diskitem ?
        file = fileref.fullPath()
        #if not forceReload:
          #object=self.getObject(file)
          #if object is not None: # object is already loaded
            #files = fileref.fullPaths()
            #for x in files:
              #if os.path.exists( x ):
                #if os.stat(x).st_mtime >= object.loadDate: # reload it if the file has been modified since last load
                  #self.reloadObjects([object])
                  #break
            #return object
        newObject=anatomistModule.Anatomist.loadObject(self, file, objectName, restrict_object_types, forceReload, duplicate, hidden)
        # search referential for this object
        if loadReferential:
          try:
            tm = registration.getTransformationManager()
            ref = tm.referential(fileref)
            if ref is not None :
              # the referential is loaded only if necessary : if the object has not the right referential assigned
              ruuid=str(ref.uuid())
              oruuid=newObject.referential.refUuid
              if (ruuid != oruuid):
                # create referential
                aref=self.createReferential(ref)
                
                ## search transformations for this referential
                #self.loadTransformations(aref)
                
                # assign referential to object
                newObject.assignReferential(aref)
                
              # search transformations for this referential
              self.loadTransformations(newObject.referential)
              
          except:
            neuroException.showException( afterError= \
              'Cannot load referential and transformations information with ' \
              'object "' + file + '"' )
      else:
        newObject=anatomistModule.Anatomist.loadObject(self,fileref, objectName, restrict_object_types, forceReload, duplicate, hidden)
      return newObject

    def createReferential(self, fileref=None):
      """
      Creates a new referential using the informations in the file.

      @type fileref: string or Diskitem
      @param filename: name or diskitem of a file (minf file, extension .referential) containing  informations about the referential : its name and uuid

      @rtype: Referential
      @return: the newly created referential
      """
      if hasattr( fileref, "fullPath" ): # is it a diskitem ?
        ruuid=fileref.uuid()
        # if it is a known referential, nothing to load
        if ruuid == registration.talairachACPCReferentialId :
          newRef=self.centralRef
          #print "getCentralReferential", newRef
        elif ruuid == registration.talairachMNIReferentialId:
          newRef=self.mniTemplateRef
          #print "getMniRef", newRef
        # unknown referential
        else:
          #print "create referential", fileref.fullPath()
          newRef=anatomistModule.Anatomist.createReferential(self, fileref.fullPath())
          #print "created referential", newRef
          newRef.diskitem=fileref
      else:
        newRef=anatomistModule.Anatomist.createReferential(self, fileref)
      return newRef

    def loadTransformations(self, referential):
      """
      Search and load transformations between this referential and spm referential.

      @type referential: Referential
      @param referential: the referential for which searching transformations
      """
      def _transformWith( self, ref1, ref2, forceLoadTransformation = True):
        def _transformWith2( self, ref1, ref2):
          tm = registration.getTransformationManager()
          if isinstance( ref1, self.Referential ):
            id1=None
            if (getattr(ref1, "diskitem", None)==None):
              id1=ref1.refUuid
            else:			     
              id1 = ref1.diskitem.uuid()              
            srcr = ref1
          else:
            id1 = ref1

          if isinstance( ref2, self.Referential ):	      		
            id2 = None
            if (getattr(ref2, "diskitem", None)==None):
              id2 = ref2.refUuid
            else:
              id2 = ref2.diskitem.uuid()                                
          else:
            id2 = ref2
            
          pth = tm.findPaths( id1, id2 )
          
          try:
            p = pth.next()
            
            loadTrAndCreateRef=forceLoadTransformation
            try:
                p = pth.next()
                self.log(string.join('processTransformations warning: multiple transformations from', ref1, 'to', ref2))
                loadTrAndCreateRef = forceLoadTransformation
            except:
                loadTrAndCreateRef = True
                        
            if(loadTrAndCreateRef == True):
              if not isinstance( ref1, self.Referential ):
                srcrDiskItem=tm.referential(id1)
                srcr=self.createReferential(srcrDiskItem)
                ref1 = srcr
            
              for t in p:
                  dstrid = t[ 'destination_referential' ]
                  if dstrid != id2 \
                    or not isinstance( ref2, self.Referential ):
                    dstrDiskItem=tm.referential(dstrid)
                    dstr=self.createReferential(dstrDiskItem)
                  else:
                    dstr=ref2
                    
                  #print "load transformation", t, ":", srcr, "->", dstr
                  self.loadTransformation( t.fullPath(), srcr, dstr )
                  srcr = dstr
          
            return loadTrAndCreateRef# 0 -> no UNIQUE path, transformations not loaded

          except StopIteration:
            return 0 # no path
        #end _transformWith2 ------------------------------------------------
            
        # try to find transformation ref1 -> ref2
        if _transformWith2( self, ref1, ref2 ):
            return 1
        # if None, try to find trnasformation ref2 -> ref1
        return _transformWith2( self, ref2, ref1 )
      #end _transformWith------------------------------------------------

      # try to find transformation between this referential and spm referential
      triedrefs = [ self.mniTemplateRef, self.centralRef,
      registration.globallyRegistredSPAMReferentialId ]      
        
      for r in triedrefs:
        x = _transformWith( self, referential, r )
        if x==1:
          return x
      
      #print "no path from/to [mni, central or spam], search for others (if no ambiguity : unique path only)."
      # try to find transformation between this referential and others
      allRefs=self.getReferentials()
      
      triedUuid=[]
      for tr in triedrefs:
        if isinstance( tr, self.Referential ):
          triedUuid.append(tr.refUuid)
        else:
          triedUuid.append(tr)
      
      toremove = []
      for r in allRefs:
        if(r.refUuid in triedUuid):
          toremove.append(r) 
              
      for r in toremove :
        allRefs.remove(r)	
      
      if referential in allRefs :
        allRefs.remove(referential)  

      otherRefs = allRefs
      
      result = 0      
      for r in otherRefs :
        x = _transformWith( self, referential, r, False)
        if x == 1: # seulement si il n y a pas ambiguite
          result = 1          
           
      return result

    def __getattr__(self, name):
      """
      Called when trying to access to name attribute, which is not defined.
      Used to give a value to centralRef attribute first time it is accessed.
      """
      if name == "centralRef":
        tm = registration.getTransformationManager()
        cr = tm.referential( registration.talairachACPCReferentialId )
        anatomistModule.Anatomist.__getattr__(self, name)
        self.lock.acquire()
        self.centralRef.diskitem=cr
        self.lock.release()
        #print "central ref loaded", self.centralRef, self.centralRef.diskitem
        return self.centralRef
      elif name == "mniTemplateRef":
        tm = registration.getTransformationManager()
        mnir = tm.referential( registration.talairachMNIReferentialId )
        anatomistModule.Anatomist.__getattr__(self, name)
        self.lock.acquire()
        self.mniTemplateRef.diskitem=mnir
        self.lock.release()
        #print "mni ref loaded", self.mniTemplateRef, self.mniTemplateRef.diskitem
        return self.mniTemplateRef
      else:
        anatomistModule.Anatomist.__getattr__(self, name)

    # util methods for brainvisa processes
    def viewObject(self, fileRef, wintype = "Axial", palette = None ): # AnatomistImageView
      """
      Loads the image in anatomist, opens it in a new window and assigns the image's referential to the window.

      @type fileRef: string or DiskItem
      @param fileRef: filename of the image to load.
      @type

      @rtype: dictionary
      @return: {"object" : the loaded object, "window" : the new window, "file" : the readed file}
      """
      if palette is not None:
        # if the palette has to be changed, load object with duplicate option : returned object will be a copy of original object, which palette will not be modified
        object = self.loadObject( fileRef, duplicate=True )
        object.setPalette(palette)
      else:
        object = self.loadObject( fileRef)
      window = self.createWindow( wintype )
      window.assignReferential(object.referential)
      window.addObjects( [object] )
      return {"object" : object, "window" : window, "file" : fileRef}

    def viewMesh(self, fileRef):
      """
      Loads a mesh and opens it in a 3D window. The window is assigned the object's referential.
      """
      return self.viewObject(fileRef, "3D")

    def viewBias(self, fileRef, forceReload=False, wintype="Axial"):
      """
      Loads an image, opens it in a new window in the object's referential. A palette is assigned to the object (Rainbow2) in order to see the bias.
      """
      object = self.loadObject( fileRef, duplicate=True )
      object.setPalette( self.getPalette("Rainbow2") )
      window = self.createWindow( wintype )
      window.assignReferential( object.referential)
      window.addObjects( [object] )
      return {"object" : object, "window" : window, "file" : fileRef}

    def viewMaskOnMRI(self, mriFile, maskFile, palette, mode, rate, wintype = "Axial" ):
      """
      Fusions an irm image with a mask. A palette is associated to the mask to highlight it in the fusion.
      Fusion method is Fusion2DMethod. Resulting image is opened in a new window.

      @type mriFile: Diskitem or string
      @param mriFile: file containing the mri image
      @type maskFile: Diskitem or string
      @param maskFile: file containing the mask to apply
      @type palette: APalette
      @param palette: palette to use for the mask
      @type mode: string
      @param mode: mix color mode (geometric, linear, replace, decal, blend, add or combine)
      @type rate: float
      @param rate: mix rate in linear mode
      @type wintype: string
      @param wintype: type of window to open
      """
      mri = self.loadObject( mriFile )
      duplicate=False
      if palette is not None:
        duplicate=True
      mask = self.loadObject( maskFile, duplicate=True)#forceReload=1 )
      mask.setPalette( palette )
      fusion = self.fusionObjects( [mri, mask], method='Fusion2DMethod' )
      self.execute("Fusion2DParams", object=fusion, mode=mode, rate = rate,
                          reorder_objects = [ mri, mask ] )
      window = self.createWindow( wintype )
      window.assignReferential( mri.referential )
      window.addObjects( [fusion] )

      return {'mri' : mri, 'mask' : mask, 'fusion' : fusion, 'window' : window, 'mriFile' : mriFile, 'maskFile' : maskFile}

    def viewActivationsOnMRI( self, mriFile, fmriFile, transformation = None, both = 0,
              palette = None, mode = "geometric",
              invertTransformation = 0,
              block = None, wintype = "Axial" ):
      if palette is None:
        palette = self.getPalette("actif_ret")
      mri = self.loadObject( mriFile )
      fmri = self.loadObject( fmriFile, duplicate=True )
      fmri.setPalette( palette, maxVal = 1, minVal = 0 )

      window = self.createWindow( wintype, block=block )
      refMRI = self.createReferential()
      self.assignReferential(refMRI, [mri, window] )

      if both:
          window2 = self.createWindow( wintype )
          refFMRI = self.createReferential()
          self.assignReferential( refFMRI, [fmri, window2] )
          window2.addObjects( [fmri] )
      else:
          refFMRI = self.createReferential()
          fmri.assignReferential(refFMRI)
      if invertTransformation:
          ref1, ref2 = refMRI, refFMRI
      else:
          ref1, ref2 = refFMRI, refMRI
      if transformation is not None:
        if type( transformation ) in ( types.ListType, types.TupleType ):
          transformation = self.createTransformation( transformation, ref1, ref2 )
        else:
          transformation = self.loadTransformation(transformation.fullPath(), ref1, ref2)
      fusion = self.fusionObjects( [mri, fmri], method = 'Fusion2DMethod' )
      self.execute("Fusion2DParams", object=fusion, mode = mode, rate = 0.5,
                            reorder_objects = [ mri, fmri ] )
      window.addObjects( [fusion] )

      # Keep a reference on objects  In case of temporary file, it must not be
      # deleted while in Anatomist
      return {"mri" : mri, "fmri" : fmri, "fusion" : fusion, "window" : window, "refMRI" : refMRI, "refFMRI" : refFMRI, "transformation" : transformation, "mriFile" : mriFile, "fmriFile" : fmriFile}

    def viewTextureOnMesh( self, meshFile, textureFile, palette=None, interpolation=None ):
      """Load a mesh file and apply the texture with the palette"""
      mesh = self.loadObject( meshFile.fullPath() )
      duplicate=False
      if palette is not None:
        duplicate=True
      tex = self.loadObject( textureFile.fullPath(), duplicate=duplicate)
      if palette is not None:
        tex.setPalette( palette )
      # Fusion indexMESH with indexTEX
      fusion = self.fusionObjects( [mesh, tex], method = 'FusionTexSurfMethod' )
      if interpolation is not None:
        self.execute("TexturingParams", objects=[fusion], interpolation = interpolation)

      window = self.createWindow( "3D" )
      window.assignReferential( mesh.referential )
      window.addObjects( [fusion] )
      # Keep a reference on mesh. In case of temporary file, it must not be
      # deleted while in Anatomist
      return {"mesh" : mesh, "texture" : tex, "fusion" : fusion, "window" : window, "meshFile" : meshFile, "textureFile" : textureFile}

    #def close( self ):
      #print 'CLOSE !!!'
      #if neuroConfig.anatomistImplementation != 'threaded':
        #print 'really really closing.'
        #anatomistModule.Anatomist.close( self )
      #else:
        #self.execute( 'DeleteAll' )
        #self.getControlWindow().close()

  # end of Anatomist class

  class AWindowChoice( neuroData.Choice ):
    '''
    A process parameter to choose an Anatomist window.
    This parameter is a choice between several anatomist windows. By default, if anatomist isn't started or if there's no opened windows, the choice offer the possibility to create any type of anatomist window.
    If anatomist is started, the set of opened window is added to the choices list.
    The list of choices is not updated automatically, to refresh the choices it is necessary to open a new instance of the process.
    '''

    def __init__( self, noSelectionLabel=None ):
      neuroData.Choice.__init__( self )
      self._init2( noSelectionLabel )

    def _init2( self, noSelectionLabel=None ):
      if noSelectionLabel is None:
        noSelectionLabel = '<'+_t_('None')+'>'
      self._initargs = ( noSelectionLabel, )
      self.noSelectionLabel = noSelectionLabel
      # initial choice : creating new windows
      self.newWindow = ( '<'+_t_('New window (3D)')+'>', self._newWindow )
      self.newWindowA = ( '<'+_t_('New window (Axial)')+'>',
                          lambda self=self: self._newWindow( 'Axial' ) )
      self.newWindowS = ( '<'+_t_('New window (Sagittal)')+'>',
                          lambda self=self: self._newWindow( 'Sagittal' ) )
      self.newWindowC = ( '<'+_t_('New window (Coronal)')+'>',
                          lambda self=self: self._newWindow( 'Coronal' ) )
      self.newWindowB = ( '<'+_t_('New window (Browser)')+'>',
                          lambda self=self: self._newWindow( 'Browser' ) )
      self.refreshChoices()

    def __getinitargs__( self ):
      """
      getinitargs, getstate, setstate are used by copy module.
      When a process is opened, its signature is copied. So if the signature contains a AWindowChoice, it is copied too. 
      If a shallow copy is done, the new AWindowChoice will contain reference from the source object and it is a source of problems. So these methods are redefined in order to make the shallow copy create a new distinct instance.
      With getinitargs, the constructor is called to create the new object. And because getstate and setstate do nothing, the source object is not copied, the two objects are distinct instances.
      """
      return self._initargs

    def __getstate__(self):
      dic = neuroData.Choice.__getstate__( self )
      dic[ 'values' ] = []
      return dic

    def __setstate__(self, state):
      neuroData.Choice.__setstate__( self, state )
      self._init2()
      
    def refreshChoices( self, *args ):
      """
      Updates choice list. Adds to default choices curently opened anatomist windows.
      """
      # if anatomist is not started, this command will not start it.
      a = Anatomist( create=False )
      if a is not None:
        try:
          windows = a.getWindows()
          choices = [ (self.noSelectionLabel,None), self.newWindow,
                      self.newWindowA, self.newWindowS, self.newWindowC,
                      self.newWindowB ] + \
            map( lambda w, self=self: \
                # (repr(w), lambda w=w: w), windows ) # label=repr(window), value=a lambda function that returns the window
                (repr(w), lambda wr=w.getRef("Weak"): wr), windows ) # label=repr(window), value=a lambda function that returns the window
                # before value was weakref.ref(w, self.refreshChoices) but it doesn't work with current anatomist api objects : the window object is not a permanant object, it is only created for the getWindows method and is destroyed at the end of refreshChoices, which calls the callback, which create a new window object (proxy for anatomist window)... -> infinite loop
          self.setChoices( *choices )
        except:
          # if it is impossible to get informations about anatomist windows, it may be already closed (close window event may occur after exit event...), so retrieve default choices.
          self.clearChoices()
      else:
        self.clearChoices()

    def clearChoices( self ):
        self.setChoices( (self.noSelectionLabel,None), self.newWindow,
                        self.newWindowA, self.newWindowS, self.newWindowC,
                        self.newWindowB )

    def _newWindow( self, type = "3D" ):
      """
      Creates a new Anatomist window of the given type.
      """
      return Anatomist().createWindow( type )

else: # if anatomist module is not available: empty classes
  class Anatomist:
    pass
  class AWindowChoice:
    pass

