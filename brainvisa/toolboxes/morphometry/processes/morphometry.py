# Copyright CEA and IFR 49 (2000-2005)
#
#  This software and supporting documentation were developed by
#      CEA/DSV/SHFJ and IFR 49
#      4 place du General Leclerc
#      91401 Orsay cedex
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

from neuroProcesses import *
from neuroData import *
from neuroDataGUI import *
import threading
from brainvisa.data.qtgui.readdiskitemGUI import DiskItemEditor
import distutils.spawn
import neuroPopen2
import os

name = 'Morphometry statistics'
userLevel = 0

labelselector = distutils.spawn.find_executable(
    'AimsLabelSelector' )
selectionmode = 1

def selectionType():
    if selectionmode == 1:
        return Selection()
    else:
        return String()


def changeSelectionMode( mode = 1 ):
    global labelselector
    if not labelselector:
        mode = 0
    global selectionmode
    selectionmode = mode
    signature[ 'region' ] = selectionType()


class Selection( Parameter ):
    def __init__( self, model = None, nomencl = None ):
        Parameter.__init__( self )
        self.value = {}
        self.fileDI = WriteDiskItem( 'Labels selection', 'selection' )
        self.file = None
        if model:
            self.value[ 'model' ] = model
        if nomencl:
            self.value[ 'nomenclature' ] = nomencl

    def findValue( self, value ):
        return value

    def editor( self, parent, name, context ):
        return SelectionEditor( parent, name )

    def listEditor( self, parent, name, context ):
        return NotImplementedEditor( parent )

    def getAutoSelection( self ):
        model = self.value.get( 'model' )
        nom = self.value.get( 'nomenclature' )
        fsel = self.file
        psel = self.value.get( 'selection' )
        cmd = 'AimsLabelSelector -b'
        if model:
            cmd += ' -m ' + model
        if nom:
            cmd += ' -n ' + nom
        if psel:
            cmd += ' -p -'
        elif fsel:
            cmd += ' -p "' + fsel.fullPath() + '"'
        stdout, stdin = neuroPopen2.popen2( cmd )
        if( psel ):
            stdin.write( psel )
            stdin.flush()
        s = stdout.read()
        stdin.close()
        stdout.close()
        return s

    def writeSelection( self, context = defaultContext() ):
        s = self.getAutoSelection()
        if not self.file:
            self.file = defaultContext().temporary( 'selection' )
        try:
            f = open( self.file.fullPath(), 'w' )
        except:
            context.write( '<b><font color="#c00000">Warning:</font></b> ' \
                         'writeSelection: file', self.file.fullPath(),
                         "can't be written<br>" )
            self.file = defaultContext().temporary( 'selection' )
            f = open( self.file.fullPath(), 'w' )
        f.write( s )
        f.close()
        return s


class SelectionEditor( QHBox, DataEditor ):
    def __init__( self, parent, name ):
        DataEditor.__init__( self )
        QHBox.__init__( self, parent, name )
        self.value = Selection()
        self._disk = DiskItemEditor( self.value.fileDI, self, 'diskitem', 1 )
        self._edit = QPushButton( '...', self, 'edit' )
        self.connect( self._edit, SIGNAL( 'clicked()' ), self.run )
        self._labelsel = 0
        self.connect( self._disk, PYSIGNAL( 'newValidValue' ),
                      self.diskItemChanged )

    def setValue( self, value, default=0 ):
        if value is not None:
            self.value = value
        else:
            self.value = Selection()

    def getValue( self ):
        return self.value

    def run( self ):
        if self._labelsel == 0:
            self._labelsel = 1
            model = self.value.value.get( 'model' )
            nom = self.value.value.get( 'nomenclature' )
            fsel = self.value.file
            psel = self.value.value.get( 'selection' )
            cmd = 'AimsLabelSelector'
            if model:
                cmd += ' -m ' + model
            if nom:
                cmd += ' -n ' + nom
            if psel:
                cmd += ' -p -'
            elif fsel:
                cmd += ' -p "' + fsel.fullPath() + '"'
            sys.stdout.flush()
            self._stdout, self._stdin = neuroPopen2.popen2( cmd )
            if( psel ):
                # print 'writing selection:', psel
                self._stdin.write( psel )
                self._stdin.flush()
            self._thread = threading.Thread( target = self.read )
            self._thread.start()
 
    def read( self ):
        val = self._stdout.read()
        sys.stdout.flush()
        del self._stdout
        del self._stdin
        if val:
            self.value.value[ 'selection' ] = val
            self.newValue()
        self._labelsel = 0
        del self._thread

    def newValue( self ):
        self.emit( PYSIGNAL('newValidValue'), ( self.name(), self.value, ) )
        #self.emit( PYSIGNAL('noDefault'), ( self.name(),) )

    def diskItemChanged( self, name, val):
        print 'Selector: file changed:', val
        self.value.file = val
        if val is None:
            print 'temp'
        else:
            file = val.fullPath()
            print file
            if self.value.value.get( 'selection' ):
                del self.value.value[ 'selection' ]


sign = (
    'model', ReadDiskItem( 'Model graph', 'Graph' ),
    'data_graphs', ListOf( ReadDiskItem( "Data graph", 'Graph' ) ),
    'nomenclature', ReadDiskItem( 'Nomenclature', 'Hierarchy' ), 
    'region', selectionType(), 
    'output_prefix', String(), 
    'region_type', Choice( ( 'Region', 'label' ), 
                           ( 'Relations with region', 'label1 label2' ), 
                           ( 'All', 'label label1 label2' ) ),
    )

if selectionmode == 0:
    sign.append( 'region_as_regexp', Boolean() )

sign += (
    'label_attribute', Choice( 'auto', 'label', 'name' ), 
    'run_dataMind', Boolean(), 
    )

signature = Signature( *sign )

def initialization( self ):
    def change_region( self, proc ):
        if self.model:
            mod = self.model.fullPath()
        else:
            mod = None
        if self.nomenclature:
            nom = self.nomenclature.fullPath()
        else:
            nom = None
        sel = self.region
        if sel is None:
            sel = Selection( mod, nom )
        else:
            sel.value[ 'model' ] = mod
            sel.value[ 'nomenclature' ] = nom
        return sel

    self.model = self.signature[ 'model' ].findValue( { 'side' : 'right' } )
    self.nomenclature = self.signature[ 'nomenclature' ].findValue( {} )
    self.setOptional( 'region_type' )
    if selectionmode == 0:
        self.region_as_regexp = 0
    self.setOptional( 'nomenclature' )
    self.setOptional( 'output_prefix' )
    self.name_descriptors = 1
    #self.print_subjects = 1
    self.print_labels = 1
    self.run_dataMind     = 0
    if selectionmode == 0:
        self.region = 'S.C.'
    else:
        self.linkParameters( 'region', ( 'model', 'nomenclature' ), \
                             change_region )

def execution( self, context ):
    context.write( "Morphometry statistics running" )
    # print self.region.value
    progname = 'siMorpho'
    tmp = context.temporary( 'Config file' )
    context.write( 'config : ', tmp.fullPath() )
    if len( self.data_graphs ) == 0:
        raise Exception( _t_( 'argument <em>data_graph</em> should not be ' \
                              'empty' ) )
    try:
        stream = open( tmp.fullPath(), 'w' )
    except IOError, (errno, strerror):
        error(strerror, maker.output)
    stream.write( '*BEGIN TREE 1.0 siMorpho\n' )
    stream.write( 'modelFile  ' + self.model.fullPath() + "\n" )
    stream.write( 'graphFiles  ' + \
                  string.join( map( lambda x: x.fullPath(), 
                  self.data_graphs ) ) + "\n" )

    if self.region_type is None:
        self.region_type = 'label'
    stream.write( 'filter_attributes  ' + self.region_type + "\n" )
    if selectionmode == 0:
        if self.region_as_regexp:
            region = self.region
        else:
            region = re.sub( '(\.|\(|\)|\[|\])', '\\\\\\1', self.region )
            region = re.sub( '\*', '.*', region )
        stream.write( 'filter_pattern  ' + region + "\n" )
    else:
        self.region.writeSelection( context )
        sfile = self.region.file
        stream.write( 'selection ' + sfile.fullPath() + '\n' )
    if not self.output_prefix is None:
        stream.write( 'output_prefix  ' + self.output_prefix + "\n" )
    if not self.nomenclature is None:
        stream.write( 'labelsMapFile  ' + self.nomenclature.fullPath() 
                      + "\n" );
    if self.label_attribute != 'auto':
        stream.write( 'label_attribute  ' + self.label_attribute + '\n' )
    if self.name_descriptors:
        stream.write( 'name_descriptors 1\n' )
    stream.write( 'descriptor_aliases ' \
                  'fold_descriptor2 fold_descriptor3\n' )
    if self.print_labels:
        stream.write( 'print_labels 1\n' )
##    if self.print_subjects:
##    stream.write( 'subject_regex [LR]\\([^/\\]*\\)\\(Base\\|Auto[0-9]*\\)\\.arg\n' )
    subjects = []
    for x in self.data_graphs:
	    s = x.get('subject')
	    if s:	s = str(s)
	    else:	s = os.path.basename(x.fullPath())
	    subjects.append(s)
    stream.write( 'subjects ' + string.join( subjects ) + "\n" )
    stream.write( '*END\n' )
    stream.close()
    f = open( tmp.fullPath() )
    context.log( 'siMorpho input file', html=f.read() )
    f.close()
    context.system( progname, tmp.fullPath() )
    #c = Command( progname + " " + tmp.fullPath() )
    #c.start()
    #sel = self.region.value.get( 'selection' )
    #if not sel:
    #    sel = 'attributes = {}\n'
    #c.stdin.write( sel )
    #result = c.wait()
    #if result:
    #    context.write( '<b>siMorpho failed: result = ' \
    #                   + str( result ) + '</b>' )

    # Run dataMind if needed
    if self.run_dataMind:
        if not distutils.spawn.find_executable( 'R' ):
            context.write( '<font color="#c00000">R is not found</font> '
                           'so the data mind module will not be run' )
        else:
            def _subj(x):
                y = x.get( 'subject' )
                if y is None:
                    y = os.path.basename( x.fileName() )
                    if y[ -4: ] == '.arg':
                        y = y[ :-4 ]
                    if y[0] == 'L' or y[0] == 'R':
                        y = y[ 1: ]
                return y
            subjects = map(lambda x: _subj(x),self.data_graphs)
            subjectsFile = context.temporary('Config file')
            try:
                f = open( subjectsFile.fullPath(), 'w' )
            except IOError, (errno, strerror):
                error(strerror, maker.output)
            else:
                f.write('subject\n')
                for subject in subjects : f.write(subject+'\n')
                f.close()
            #context.write( "DataMind running" )
            python_interpretor = sys.executable
            progname =  [ python_interpretor,
                          os.path.join(os.path.join(mainPath,'bin'),
                                       'datamind'),
                          subjectsFile.fullPath(),
                          str(self.output_prefix) ]
            context.write( 'Running ', *progname)
            context.system( *progname )

#enable selector
changeSelectionMode( 1 )

