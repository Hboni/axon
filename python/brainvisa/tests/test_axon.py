import unittest
import doctest
import os
import tempfile
import shutil

# set en empty temporary user dir
# BRAINVISA_USER_DIR soult be set before neuroConfig is imported
homedir = tempfile.mkdtemp(prefix='bv_home')
os.environ['BRAINVISA_USER_DIR'] = homedir

import brainvisa.tests.test_history
import brainvisa.tests.test_registration

import brainvisa.axon
from brainvisa.configuration import neuroConfig
from brainvisa.data import neuroHierarchy
from brainvisa.processes import defaultContext

def setup_doctest(test):
    brainvisa.axon.initializeProcesses()
    # set english language because doctest tests the output of commands which
    # can be different according to the selected language
    neuroConfig.language = "en"
    neuroConfig.__builtin__.__dict__['_t_'] \
        = neuroConfig.Translator(neuroConfig.language).translate
    # update share database
    db = list(neuroHierarchy.databases.iterDatabases())[0]
    db.clear(context=defaultContext())
    db.update(context=defaultContext())
    brainvisa.axon.cleanup()

def teardown_doctest(test):
    pass

def test_suite():
    suite =unittest.TestSuite()
    doctest_suite = unittest.TestSuite(doctest.DocFileSuite(
        "usecases.rst", setUp=setup_doctest,
        tearDown=teardown_doctest,
        optionflags=doctest.ELLIPSIS))
    suite.addTest(doctest_suite)
    suite.addTest(brainvisa.tests.test_history.test_suite())
    suite.addTest(brainvisa.tests.test_registration.test_suite())
    return suite

try:
    if __name__ == '__main__':
        unittest.main(defaultTest='test_suite')
finally:
    shutil.rmtree(homedir)
    del homedir

# WARNING: if this file is imported as a module, homedir will be removed,
# and later processing will issue errors
