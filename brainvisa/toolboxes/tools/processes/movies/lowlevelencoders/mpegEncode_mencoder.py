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

from brainvisa.processes import *
from brainvisa.validation import ValidationError
from brainvisa.configuration import mpegConfig
import os
from brainvisa.tools import aimsGlobals

name = 'Mencoder MPEG encoder'
userLevel = 2


def validation():
    if 'mencoder' not in mpegConfig.encoders:
        raise ValidationError(_t_('Mencoder not present'))


def codecs():
    c = mpegConfig.codecs.get('mencoder')
    if c is not None:
        return c
    return {}


signature = Signature(
    'images', ListOf(ReadDiskItem('2D Image', 'aims Image Formats',
                                  ignoreAttributes=1)),
    'animation', WriteDiskItem('MPEG film', mpegConfig.mpegFormats),
    'encoding', Choice(*codecs()),
    'quality', Integer(),
    'framesPerSecond', Integer(),
    'passes', Choice(1, 2),
    'additional_encoder_options', ListOf(String()),
)


def initialization(self):
    self.quality = 75
    self.framesPerSecond = 25
    self.passes = 1


def execution(self, context):
    if len(self.images) == 0:
        raise RuntimeError('No image selected')
    context.write(self.images[0])
    attrs = aimsGlobals.aimsVolumeAttributes(self.images[0], forceFormat=1)
    context.write(attrs)
    width = attrs['volume_dimension'][0]

    im = [x.fullPath() for x in self.images]
    passlog = context.temporary('log file')
    cmd = ['mencoder', 'mf://' + ','.join(im),
           '-o', self.animation.fullPath(), '-mf',
           'fps=' + str(self.framesPerSecond), '-ovc', 'lavc',
           '-passlogfile', passlog, '-lavcopts',
           'vbitrate=' + str(self.quality * 10) + ':vcodec='
           + self.encoding + ':vpass=1'] + self.additional_encoder_options
    # print cmd
    context.system(*cmd)
    if self.passes > 1:
        cmd[-1] = cmd[-1][:-1] + '2'
        # print cmd
        context.system(*cmd)
