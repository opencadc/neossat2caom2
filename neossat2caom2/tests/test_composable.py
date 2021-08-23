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

from datetime import datetime, timedelta
from mock import patch, Mock, ANY

from caom2 import SimpleObservation
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from neossat2caom2 import composable, APPLICATION, NEOSSatName, NEOS_BOOKMARK
from neossat2caom2 import scrape

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
STATE_FILE = os.path.join(TEST_DATA_DIR, 'state.yml')
START_TIME = datetime.utcnow()
TEST_START_TIME = (START_TIME - timedelta(days=2)).isoformat()
TEST_TIMESTAMP_1 = (START_TIME - timedelta(days=1)).timestamp()
TEST_TIMESTAMP_2 = (START_TIME - timedelta(hours=12)).timestamp()


TEST_FILE_LIST = [
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268004930_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268004930.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005240_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005240.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005550_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005550.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005900_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268005900.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268010210_clean.fits',
    '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/'
    'NEOS_SCI_2019268010210.fits',
]


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch('neossat2caom2.scrape._append_todo')
def test_run_state(query_mock, run_mock, access_mock):
    access_mock.return_value = 'https://localhost'
    _write_state(TEST_START_TIME)
    query_mock.side_effect = _append_todo_mock
    run_mock.return_value = 0
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)
    try:
        test_result = composable._run_state()
        assert test_result == 0, 'expected successful test result'
        assert run_mock.called, 'should be called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, NEOSSatName), type(test_storage)
        assert test_storage.file_name.startswith(
            'NEOS_SCI'
        ) and test_storage.file_name.endswith('.fits'), test_storage.file_name
        assert run_mock.call_count == 10, 'wrong call count'
    except Exception as e:
        assert False, f'unexpected exception {e}'
    finally:
        os.getcwd = getcwd_orig


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2utils.data_util.StorageClientWrapper')
def test_run_by_file(data_client_mock, repo_mock, access_mock):
    access_mock.return_value = 'https://localhost'
    repo_mock.return_value.read.side_effect = _mock_repo_read
    repo_mock.return_value.create.side_effect = Mock()
    repo_mock.return_value.update.side_effect = _mock_repo_update
    data_client_mock.return_value.info.side_effect = (
        _mock_get_file_info
    )
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)
    try:
        _write_todo()
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
        # ClientVisit executor only with the test configuration
        assert repo_mock.return_value.update.called, 'expect update mock call'
        assert (
            repo_mock.return_value.update.call_count == 10
        ), 'wrong number of mock calls'
    except Exception as e:
        assert False, f'unexpected exception {e}'
    finally:
        os.getcwd = getcwd_orig


def test_store():
    test_config = mc.Config()
    test_config.logging_level = 'DEBUG'
    test_config.working_directory = '/tmp'
    test_fqn = (
        '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/'
        '268/NEOS_SCI_2019268004930_clean.fits'
    )
    test_builder = nbc.GuessingBuilder(NEOSSatName)
    test_storage_name = test_builder.build(test_fqn)
    import logging
    logging.error(test_storage_name)
    transferrer = Mock()
    cadc_data_client = Mock()
    observable = mc.Observable(
        mc.Rejected('/tmp/rejected.yml'), mc.Metrics(test_config)
    )
    test_subject = ec.Store(
        test_config,
        test_storage_name,
        APPLICATION,
        cadc_data_client,
        observable,
        transferrer,
    )
    test_subject.execute(None)
    assert cadc_data_client.put.called, 'expect a put call'
    cadc_data_client.put.assert_called_with(
        '/tmp/2019268004930',
        'cadc:NEOSSAT/NEOS_SCI_2019268004930_clean.fits',
        None,
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    transferrer.get.assert_called_with(
        test_fqn, '/tmp/2019268004930/NEOS_SCI_2019268004930_clean.fits'
    ), 'wrong get args'


def _append_todo_mock(ignore1, ignore2, ignore3, ignore4, ignore5, ignore6):
    temp = {}
    for f_name in TEST_FILE_LIST[:8]:
        temp[f_name] = [False, TEST_TIMESTAMP_1]
    for f_name in TEST_FILE_LIST[8:]:
        temp[f_name] = [False, TEST_TIMESTAMP_2]
    return temp


def _check_execution(run_mock):
    import logging

    logging.error('called')
    assert run_mock.called, 'should have been called'
    args, kwargs = run_mock.call_args
    assert args[3] == APPLICATION, 'wrong command'
    test_storage = args[2]
    assert isinstance(test_storage, NEOSSatName), type(test_storage)
    assert test_storage.file_name.startswith(
        'NEOS_SCI'
    ) and test_storage.file_name.endswith('.fits'), test_storage.file_name
    assert run_mock.call_count == 10, 'wrong call count'


def _write_state(start_time_str):
    test_time = datetime.strptime(start_time_str, mc.ISO_8601_FORMAT)
    test_bookmark = {
        'bookmarks': {NEOS_BOOKMARK: {'last_record': test_time}},
        'context': {scrape.NEOS_CONTEXT: ['NEOSS', '2017', '2018', '2019']},
    }
    mc.write_as_yaml(test_bookmark, STATE_FILE)


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


def _mock_get_file_info(archive, file_id):
    if '_prev' in file_id:
        return {'type': 'image/jpeg'}
    else:
        return {'type': 'application/fits'}
