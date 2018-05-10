''' Specialized Process class to link with CAPSUL processes and pipelines.

the aim is to allow using a Capsul process/pipeline as an Axon process (or at least, ease it). Such a process would like the following:

::

    from brainvisa.processes import *
    from brainvisa.processing import capsul_process

    name = 'A Capusl process ported to Axon'
    userLevel = 0
    base_class = capsul_process.CapsulProcess
    capsul_process = 'morphologist.capsul.morphologist.Morphologist'

Explanation:

The process should inherit the CapsulProcess class (itself inheriting the standard Process). To do so, it declares the "base_class" variable to this CapsulProcess class type.

The it should instantiate the appropriate Capsul process. This is done by overloading the setup_capsul_process() method, which will instantiate the Capsul process and set it into the Axon proxy process.

The underlying Capsul process traits will be exported to the Axon signature automatically. This behaviour can be avoided or altered by overloading the initialize() method, which we did not define in the above example.

The process also does not have an execution() function. This is normal: CapsulProcess already defines an executionWorkflow() method which will generate a Soma-Workflow workflow which will integrate in the process or parent pipeline (or iteration) workflow.

See also :doc:`capsul`

'''

from __future__ import print_function

import os
import brainvisa.processes as processes
from brainvisa.data import neuroData
from brainvisa.data.readdiskitem import ReadDiskItem
from brainvisa.data.writediskitem import WriteDiskItem
from brainvisa.processes import getAllFormats
from brainvisa.data.neuroData import Signature
from brainvisa.data.neuroDiskItems import DiskItem, getAllDiskItemTypes
from brainvisa.data import neuroHierarchy
from brainvisa.configuration import neuroConfig
from brainvisa.configuration import axon_capsul_config_link
from soma.functiontools import SomaPartial
from traits import trait_types
import traits.api as traits
import distutils.spawn
import six


def fileOptions(filep, name, process, attributes=None, path_completion=None):
    if hasattr(filep, 'output') and filep.output:
        return (WriteDiskItem, get_best_type(process, name, attributes,
                                             path_completion))
    return (ReadDiskItem, get_best_type(process, name, attributes,
                                        path_completion))


def choiceOptions(choice, name, process, attributes=None,
                  path_completion=None):
    return [x for x in choice.trait_type.values]


def listOptions(param, name, process, attributes=None, path_completion=None):
    item_type = param.inner_traits[0]
    return [make_parameter(item_type, name, process, attributes,
                           path_completion)]


param_types_table = \
    {
        trait_types.Bool: neuroData.Boolean,
        trait_types.CBool: neuroData.Boolean,
        trait_types.String: neuroData.String,
        trait_types.Str: neuroData.String,
        trait_types.CStr: neuroData.String,
        trait_types.Float: neuroData.Number,
        trait_types.CFloat: neuroData.Number,
        trait_types.Int: neuroData.Integer,
        trait_types.CInt: neuroData.Integer,
        trait_types.File: fileOptions,
        trait_types.Directory: fileOptions,
        trait_types.Enum: (neuroData.Choice, choiceOptions),
        trait_types.List: (neuroData.ListOf, listOptions),
        trait_types.ListFloat: (neuroData.ListOf, listOptions),
        trait_types.Set: (neuroData.ListOf, listOptions),
    }


def make_parameter(param, name, process, attributes=None,
                   path_completion=None):
    newtype = param_types_table.get(type(param.trait_type))
    paramoptions = []
    kwoptions = {}
    if newtype is None:
        print('no known converted type for', name, ':',
              type(param.trait_type))
        newtype = neuroData.String
    if isinstance(newtype, tuple):
        paramoptions = newtype[1](param, name, process, attributes,
                                  path_completion)
        newtype = newtype[0]
    elif hasattr(newtype, 'func_name'):
        newtype, paramoptions = newtype(param, name, process, attributes,
                                        path_completion)
    if param.groups:
        section = param.groups[0]
        kwoptions['section'] = section
    return newtype(*paramoptions, **kwoptions)


def convert_capsul_value(value):
    if isinstance(value, (traits.TraitListObject, traits.TraitSetObject)):
        value = [convert_capsul_value(x) for x in value]
    elif value is traits.Undefined or value in ("<undefined>", "None"):
        # FIXME: "<undefined>" or "None" is a bug in the Controller GUI
        value = None
    return value


def convert_to_capsul_value(value, item_type=None, trait=None):
    if isinstance(value, DiskItem):
        value = value.fullPath()
    elif isinstance(value, list):
        value = [convert_to_capsul_value(x, item_type.contentType)
                 for x in value]
        if trait is not None and type(trait.trait_type) is trait_types.Set:
            value = set(value)
    elif isinstance(value, set):
        value = set([convert_to_capsul_value(x, item_type.contentType)
                     for x in value])
    elif value is None and isinstance(item_type, ReadDiskItem):
        value = traits.Undefined
    return value


def get_initial_study_config():
    from soma.wip.application.api import Application as Appli2
    configuration = Appli2().configuration
    init_study_config = {
        "input_fom": "morphologist-auto-1.0",
        "output_fom": "morphologist-auto-1.0",
        "shared_fom": "shared-brainvisa-1.0",
        "use_soma_workflow": True,
        "use_fom": True,
        "volumes_format": 'NIFTI gz',
        "meshes_format": "GIFTI",
    }

    return init_study_config


def match_ext(capsul_exts, axon_formats):
    if not capsul_exts:
        return True
    for format in axon_formats:
        if not hasattr(format, 'patterns'):
            continue  # probably a group
        for pattern in format.patterns.patterns:
            f0 = pattern.pattern.split('|')[-1]
            f1 = f0.split('*')[-1]
            if f1 in capsul_exts:
                return True
    return False


def get_best_type(process, param, attributes=None, path_completion=None):
    from capsul.attributes.completion_engine import ProcessCompletionEngine
    from capsul.attributes.attributes_schema import ProcessAttributes

    completion_engine = ProcessCompletionEngine.get_completion_engine(process)
    cext = process.trait(param).allowed_extensions

    if path_completion is None:
        try:
            path_completion = completion_engine.get_path_completion_engine()
        except RuntimeError:
            return ('Any Type',
                    [f for f in getAllFormats() if match_ext(cext, [f])])

    if attributes is None:
        orig_attributes = completion_engine.get_attribute_values()
        attributes = orig_attributes.export_to_dict()
        for attr, trait in six.iteritems(attributes.user_traits()):
            if isinstance(trait.trait_type, traits.Str):
                setattr(attributes, attr, '<%s>' % attr)

    path = path_completion.attributes_to_path(process, param, attributes)
    if path is None:
        # fallback to the completed value
        path = getattr(process, param)
    if path in (None, traits.Undefined):
        # print('no path for', process.name, param)
        return ('Any Type',
                [f for f in getAllFormats() if match_ext(cext, [f])])

    for db in neuroHierarchy.databases.iterDatabases():
        for typeitem in getAllDiskItemTypes():
            rules = db.fso.typeToPatterns.get(typeitem)
            if rules:
                for rule in rules:
                    pattern = rule.pattern.pattern
                    cpattern = pattern.replace('{', '<')
                    cpattern = cpattern.replace('}', '>')
                    if path.startswith(cpattern):
                        if len(cpattern) < len(path):
                            if path[len(cpattern)] != '.':
                                continue
                            if not match_ext(cext, rule.formats):
                                continue
                        return (typeitem.name, rule.formats)

    # print('no type found for', param)
    return ('Any Type',
            [f for f in getAllFormats() if match_ext(cext, [f])])


class CapsulProcess(processes.Process):

    ''' Specialized Process to link with a CAPSUL process or pipeline.

    See the :py:mod:`brainvisa.processing.capsul_process` doc for details.
    '''

    def __init__(self):
        self._capsul_process = None
        self.setup_capsul_process()
        super(CapsulProcess, self).__init__()

    def set_capsul_process(self, process):
        ''' Sets a CAPSUL process into the Axon (proxy) process
        '''
        from capsul.attributes.completion_engine import ProcessCompletionEngine

        self._capsul_process = process
        completion_engine \
            = ProcessCompletionEngine.get_completion_engine(process)

    def setup_capsul_process(self):
        ''' This method is in charge of instantiating the appropriate CAPSUL process or pipeline, and setting it into the Axon process (self), using the set_capsul_process() method.

        It may be overloaded by children processes, but the default implementation looks for a variable "capsul_process" in the process source file which provides the Capsul module/process name (as a string), for instance:

        ::

          capsul_process = "morphologist.capsul.axon.t1biascorrection.T1BiasCorrection"

        This is basically the only thing the process must do.
        '''
        module = processes._processModules[self._id]
        capsul_process = getattr(module, 'capsul_process')
        if capsul_process:
            from capsul.api import get_process_instance
            process = get_process_instance(capsul_process,
                                           self.get_study_config())
            self.set_capsul_process(process)

    def get_capsul_process(self):
        ''' Get the underlying CAPSUL process '''
        return self._capsul_process

    def initialization(self):
        ''' This specialized initialization() method sets up a default signature for the process, duplicating every user trait of the underlying CAPSUL process.

        As some parameter types and links will not be correctly translated, it is possible to prevent this automatic behaviour, and to setup manually
        a new signature, by overloading the initialization() method.

        In such a case, the process designer will also probably have to overload the propagate_parameters_to_capsul() method to setup the underlying Capsul process parameters from the Axon one, since there will not be a direct correspondance any longer.
        '''
        process = self.get_capsul_process()

        # speedup attributes
        from capsul.attributes.completion_engine import ProcessCompletionEngine
        from capsul.attributes.attributes_schema import ProcessAttributes

        completion_engine = ProcessCompletionEngine.get_completion_engine(
            process)
        try:
            path_completion = completion_engine.get_path_completion_engine()
        except RuntimeError:
            path_completion = None

        # save parameters values
        orig_params = process.export_to_dict()

        attributes = completion_engine.get_attribute_values()
        orig_attributes = attributes.export_to_dict()
        for attr, trait in six.iteritems(attributes.user_traits()):
            if isinstance(trait.trait_type, traits.Str):
                setattr(attributes, attr, '<%s>' % attr)
        #
        completion_engine.complete_parameters()

        signature_args = []
        excluded_traits = set(('nodes_activation', 'visible_groups',
                               'pipeline_steps'))
        optional = []
        for name, param in six.iteritems(process.user_traits()):
            if name in excluded_traits:
                continue
            parameter = make_parameter(param, name, process, attributes,
                                       path_completion)
            signature_args += [name, parameter]
            if param.optional:
                optional.append(name)

        # restore attributes
        for attr, trait in six.iteritems(attributes.user_traits()):
            if isinstance(trait.trait_type, traits.Str):
                setattr(attributes, attr, orig_attributes[attr])

        signature = Signature(*signature_args)
        signature['edit_pipeline'] = neuroData.Boolean()
        has_steps = False
        if getattr(process, 'pipeline_steps', None):
            has_steps = True
            signature['edit_pipeline_steps'] = neuroData.Boolean()
        signature['edit_study_config'] = neuroData.Boolean()
        self.__class__.signature = signature
        self.changeSignature(signature)

        # restore parameters
        for param, value in six.iteritems(orig_params):
            if param not in ('pipeline_steps', 'nodes_activation'):
                setattr(process, param, value)

        # setup callbacks to sync capsul and axon parameters values
        if neuroConfig.gui:
            from soma.qt_gui import qt_backend
            qt_backend.init_traitsui_handler()
            dispatch = 'ui'
        else:
            dispatch = 'same'

        self._capsul_process.on_trait_change(self._process_trait_changed,
                                             dispatch=dispatch)

        if optional:
            self.setOptional(*optional)
        self.edit_pipeline = False
        self.edit_pipeline_steps = False
        self.edit_study_config = False
        for name in process.user_traits():
            if name in excluded_traits:
                continue
            value = getattr(process, name)
            if value not in (traits.Undefined, ''):
                setattr(self, name, convert_capsul_value(value))
            else:
                setattr(self, name, None)
            self.linkParameters(None, name,
                                SomaPartial(self._on_axon_parameter_changed,
                                            name))

        self.linkParameters(None, 'edit_pipeline', self._on_edit_pipeline)
        if has_steps:
            self.linkParameters(None, 'edit_pipeline_steps',
                                self._on_edit_pipeline_steps)
        self.linkParameters(None, 'edit_study_config',
                            self._on_edit_study_config)
        if hasattr(process, 'visible_groups'):
            self.visible_sections = process.visible_groups

    def propagate_parameters_to_capsul(self):
        ''' Set the underlying Capsul process parameters values from the Axon process (self) parameters values.

        This method will be called before execution to build the workflow.

        By default, it assumes a direct correspondance between Axon and Capsul processes parameters, so it will just copy all parameters values. If the initialization() method has been specialized in a particular process, this direct correspondance will likely be broken, so this method should also be overloaded.
        '''
        process = self.get_capsul_process()
        for name, itype in six.iteritems(self.signature):
            converted_value = convert_to_capsul_value(getattr(self, name),
                                                      itype,
                                                      process.trait(name))
            try:
                setattr(process, name, converted_value)
            except traits.TraitError:
                pass

    def executionWorkflow(self, context=processes.defaultContext()):
        ''' Build the workflow for execution. The workflow will be integrated in the parent pipeline workflow, if any.

        StudyConfig options are handled to support local or remote execution, file transfers / translations and other specific stuff.

        FOM completion is not performed yet.
        '''

        from capsul.pipeline import pipeline_workflow
        from capsul.attributes.completion_engine import ProcessCompletionEngine

        study_config = self.get_study_config(context)

        self.propagate_parameters_to_capsul()
        process = self.get_capsul_process()

        completion_engine \
            = ProcessCompletionEngine.get_completion_engine(process)
        if completion_engine is not None:
            completion_engine.complete_parameters()

        wf = pipeline_workflow.workflow_from_pipeline(
            process, study_config=study_config)  # , jobs_priority=priority)
        jobs = wf.jobs
        dependencies = wf.dependencies
        root_group = wf.root_group

        return jobs, dependencies, root_group

    def init_study_config(self, context=processes.defaultContext()):
        ''' Build a Capsul StudyConfig object if not present in the context,
        set it up, and return it
        '''
        from capsul.study_config.study_config import StudyConfig
        from soma.wip.application.api import Application as Appli2

        study_config = getattr(context, 'study_config', None)

        # axon_to_capsul_formats = {
            #'NIFTI-1 image': "NIFTI",
            #'gz compressed NIFTI-1 image': "NIFTI gz",
            #'GIS image': "GIS",
            #'MINC image': "MINC",
            #'SPM image': "SPM",
            #'GIFTI file': "GIFTI",
            #'MESH mesh': "MESH",
            #'PLY mesh': "PLY",
            #'siRelax Fold Energy': "Energy",
        #}

        ditems = [(name, item) for name, item in six.iteritems(self.signature)
                  if isinstance(item, DiskItem)]
        database = ''
        # format = ''
        for item in ditems:
            # format = axon_to_capsul_formats.get(
                # item[1].format.name, item[1].format.name)
            database = getattr(self, item[0]).get('_database', '')
            if database and \
                    not neuroHierarchy.databases.database(database).builtin:
                break
            database = ''

        if study_config is None:
            initial_study_config = get_initial_study_config()
            study_config = StudyConfig(
                init_config=initial_study_config,
                modules=StudyConfig.default_modules + ['BrainVISAConfig',
                                                       'FomConfig',
                                                       'FreeSurferConfig'])
        study_config.axon_link = \
            axon_capsul_config_link.AxonCapsulConfSynchronizer(study_config)
        study_config.axon_link.sync_axon_to_capsul()
        study_config.on_trait_change(
            study_config.axon_link.sync_capsul_to_axon)

        study_config.input_directory = database
        study_config.output_directory = database

        # soma-workflow execution settings
        soma_workflow_config = getattr(context, 'soma_workflow_config', {})
        study_config.somaworkflow_computing_resource = 'localhost'
        study_config.somaworkflow_computing_resources_config.localhost \
            = soma_workflow_config

        return study_config

    def get_study_config(self, context=processes.defaultContext()):
        study_config = None
        if self._capsul_process is not None:
            study_config = getattr(self._capsul_process, 'study_config', None)
        if study_config is None:
            study_config = self.init_study_config(context)
            if self._capsul_process is not None:
                self._capsul_process.set_study_config(study_config)
        context.study_config = study_config
        return study_config

    def _on_edit_pipeline(self, process, dummy):
        from brainvisa.configuration import neuroConfig
        if not neuroConfig.gui:
            return
        if process.edit_pipeline:
            processes.mainThreadActions().push(self._open_pipeline)
        else:
            self._pipeline_view = None

    def _on_edit_pipeline_steps(self, process, dummy):
        from brainvisa.configuration import neuroConfig
        if not neuroConfig.gui:
            return
        steps = getattr(self._capsul_process, 'pipeline_steps', None)
        if not self.edit_pipeline_steps or not steps:
            steps_view = getattr(self, '_steps_view', None)
            if steps_view is not None:
                steps_view.ref().close()
                del steps_view
                del self._steps_view
            return
        from soma.qt_gui.controller_widget import ScrollControllerWidget
        from soma.qt_gui.qtThread import MainThreadLife
        steps_view = ScrollControllerWidget(steps, live=True)
        steps_view.show()
        self._steps_view = MainThreadLife(steps_view)

    def _open_pipeline(self):
        from brainvisa.configuration import neuroConfig
        if not neuroConfig.gui:
            return
        from capsul.qt_gui.widgets import PipelineDevelopperView
        from capsul.pipeline.pipeline import Pipeline
        from soma.qt_gui.qtThread import MainThreadLife
        Pipeline.hide_nodes_activation = False
        self.propagate_parameters_to_capsul()
        mpv = PipelineDevelopperView(
            self.get_capsul_process(), allow_open_controller=True,
          show_sub_pipelines=True)
        mpv.show()
        self._pipeline_view = MainThreadLife(mpv)

    def _process_trait_changed(self, name, new_value):
        if name == 'trait_added' \
                or name not in self._capsul_process.user_traits().keys():
            return
        try:
            self.setValue(name, convert_capsul_value(new_value))
        except Exception as e:
            print('exception in _process_trait_changed:', e)
            print('param:', name, ', value:', new_value, 'of type:',
                  type(new_value))
            import traceback
            traceback.print_exc()

    def _on_edit_study_config(self, process, dummy):
        from brainvisa.configuration import neuroConfig
        if not neuroConfig.gui:
            return
        if process.edit_study_config:
            processes.mainThreadActions().push(self._open_study_config_editor)
        else:
            self._study_config_editor = None

    def _open_study_config_editor(self):
        from soma.qt_gui.controller_widget import ScrollControllerWidget
        from soma.qt_gui.qtThread import MainThreadLife
        study_config = self.get_study_config()
        scv = ScrollControllerWidget(study_config, live=True)
        scv.show()
        self._study_config_editor = MainThreadLife(scv)

    def _on_axon_parameter_changed(self, param, process, dummy):
        from capsul.attributes.completion_engine import ProcessCompletionEngine

        if getattr(self, '_capsul_process', None) is None:
            return

        if getattr(self, '_ongoing_completion', False):
            return
        self._ongoing_completion = True
        try:
            value = getattr(self, param, None)
            itype = self.signature.get(param)
            trait = self._capsul_process.trait(param)
            capsul_value = convert_to_capsul_value(
                value, itype, self._capsul_process.trait(param))
            if capsul_value == getattr(self._capsul_process, param):
                return  # not changed
            setattr(self._capsul_process, param, capsul_value)
            if not isinstance(itype, ReadDiskItem):
                # not a DiskItem: nothing else to do.
                return

            completion_engine = ProcessCompletionEngine.get_completion_engine(
                self._capsul_process)
            if completion_engine is not None and isinstance(value, DiskItem):
                attributes = value.hierarchyAttributes()
                capsul_attr = completion_engine.get_attribute_values()
                param_attr \
                    = capsul_attr.get_parameters_attributes().get(param) \
                    or capsul_attr.user_traits().keys()
                if param_attr:
                    modified = False
                    for attribute, value in six.iteritems(attributes):
                        if attribute in param_attr:
                            if getattr(capsul_attr, attribute) != value:
                                setattr(capsul_attr, attribute, value)
                                modified = True

                    database = attributes.get('_database', None)
                    if database:
                        if isinstance(itype, WriteDiskItem):
                            db = ['output_directory', 'input_directory']
                        else:
                            allowed_db = [h.name
                                          for h in neuroHierarchy.hierarchies()
                                          if h.fso.name
                                          not in ("shared", "spm", "fsl")]
                            if database in allowed_db:
                                db = ['input_directory', 'output_directory']
                            else:
                                db = None
                            if db is not None:
                                study_config \
                                    = self._capsul_process.get_study_config()
                                for idb in db:
                                    if getattr(study_config, idb) != database:
                                        modified = True
                                        setattr(study_config, idb, database)

                    if modified:
                        completion_engine.complete_parameters()

        finally:
            self._ongoing_completion = False
