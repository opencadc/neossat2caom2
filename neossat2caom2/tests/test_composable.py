# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import os

from collections import defaultdict
from datetime import datetime, timedelta
from dateutil import tz
from mock import patch, Mock, PropertyMock

from caom2 import SimpleObservation
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from neossat2caom2 import composable
from neossat2caom2.main_app import APPLICATION
from neossat2caom2.storage_name import NEOSSatName

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
STATE_FILE = os.path.join(TEST_DATA_DIR, 'state.yml')
START_TIME = datetime.utcnow().replace(tzinfo=tz.UTC)
TEST_START_TIME = START_TIME - timedelta(days=2)
TEST_DT_1 = (START_TIME - timedelta(days=1))
TEST_DT_2 = (START_TIME - timedelta(hours=12))


TEST_FILE_LIST = [
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005240_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005240.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005550_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005550.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005900_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005900.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268010210_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268010210.fits',
]

x = defaultdict(list)
for f_name in TEST_FILE_LIST[:8]:
    x[TEST_DT_1].append(f_name)
for f_name in TEST_FILE_LIST[8:]:
    x[TEST_DT_2].append(f_name)


@patch('neossat2caom2.data_source.CSADataSource.todo_list', new_callable=PropertyMock(return_value=x))
@patch('neossat2caom2.data_source.CSADataSource.max_time', new_callable=PropertyMock(return_value=TEST_DT_2))
@patch('neossat2caom2.data_source.CSADataSource.initialize_todo')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_state(run_mock, access_mock, initialize_mock, max_mock, list_mock, test_config, tmpdir):
    access_mock.return_value = 'https://localhost'

    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.proxy_file_name = 'textproxy.pem'
    test_config.interval = 200
    test_config.write_to_file(test_config)

    mc.State.write_bookmark(test_config.state_fqn, composable.NEOS_BOOKMARK, TEST_START_TIME)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    run_mock.return_value = 0
    try:
        test_result = composable._run_state()
    except Exception as e:
        import traceback
        import logging
        logging.error(traceback.format_exc())
        assert False, f'unexpected exception {e}'
    assert test_result == 0, 'expected successful test result'
    assert initialize_mock.called, 'initialize should be called'
    # called for every invocation of get_time_box_work
    assert initialize_mock.call_count == 12, 'wrong initialize call count'
    assert run_mock.called, 'should be called'
    args, kwargs = run_mock.call_args
    test_storage = args[0]
    assert isinstance(test_storage, NEOSSatName), type(test_storage)
    assert test_storage.file_name.startswith(
        'NEOS_SCI'
    ) and test_storage.file_name.endswith('.fits'), test_storage.file_name
    assert run_mock.call_count == 10, 'wrong call count'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_by_file(do_one_mock, clients_mock, access_mock):
    access_mock.return_value = 'https://localhost'
    do_one_mock.return_value = 0
    clients_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    clients_mock.return_value.metadata_client.create.side_effect = Mock()
    clients_mock.return_value.metadata_client.update.side_effect = _mock_repo_update
    clients_mock.return_value.data_client.info.side_effect = (
        _mock_get_file_info
    )
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)
    try:
        _write_todo()
        try:
            test_result = composable._run()
        except Exception as e:
            assert False, f'unexpected exception {e}'
        assert test_result == 0, 'wrong result'
        # ClientVisit executor only with the test configuration
        assert do_one_mock.called, 'expect do_one call'
        assert do_one_mock.call_count == 10, 'wrong number of mock calls'
    finally:
        os.getcwd = getcwd_orig


def test_store():
    tc = mc.Config()
    tc.logging_level = 'DEBUG'
    tc.working_directory = '/tmp'
    test_fqn = (
        '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/'
        '268/NEOS_SCI_2019268004930_clean.fits'
    )
    test_builder = nbc.GuessingBuilder(NEOSSatName)
    test_storage_name = test_builder.build(test_fqn)
    transferrer = Mock()
    clients_mock = Mock()
    metadata_reader_mock = Mock()
    observable = mc.Observable(
        mc.Rejected('/tmp/rejected.yml'), mc.Metrics(tc)
    )
    test_subject = ec.Store(tc, observable, transferrer, clients_mock, metadata_reader_mock)
    test_subject.execute({'storage_name': test_storage_name})
    assert clients_mock.data_client.put.called, 'expect a put call'
    clients_mock.data_client.put.assert_called_with(
        '/tmp/2019268004930',
        'cadc:NEOSSAT/NEOS_SCI_2019268004930_clean.fits',
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    transferrer.get.assert_called_with(
        test_fqn, '/tmp/2019268004930/NEOS_SCI_2019268004930_clean.fits'
    ), 'wrong get args'


def _check_execution(run_mock):
    assert run_mock.called, 'should have been called'
    args, kwargs = run_mock.call_args
    assert args[3] == APPLICATION, 'wrong command'
    test_storage = args[2]
    assert isinstance(test_storage, NEOSSatName), type(test_storage)
    assert test_storage.file_name.startswith(
        'NEOS_SCI'
    ) and test_storage.file_name.endswith('.fits'), test_storage.file_name
    assert run_mock.call_count == 10, 'wrong call count'


def _write_todo():
    test_config = mc.Config()
    test_config.get_executors()
    with open(test_config.work_fqn, 'w') as f:
        for f_name in TEST_FILE_LIST:
            f.write('{}\n'.format(f_name))


def _mock_repo_read(collection, obs_id):
    return SimpleObservation(collection=collection, observation_id=obs_id)


def _mock_repo_update(ignore1):
    return None


def _mock_get_file_info(uri):
    if '_prev' in uri:
        return {'type': 'image/jpeg'}
    else:
        return {'type': 'application/fits'}
