# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2025.                            (c) 2025.
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
import re

from datetime import datetime, timedelta
from mock import ANY, patch, Mock

from cadcdata import FileInfo
from caom2 import SimpleObservation
from caom2pipe.html_data_source import HttpDataSourceRunnerMeta
from caom2pipe.astro_composable import make_headers_from_file
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from neossat2caom2 import composable
from neossat2caom2.storage_name import NEOSSatName

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
START_TIME = datetime.now()
TEST_START_TIME = START_TIME - timedelta(days=2)
TEST_DT_1 = START_TIME - timedelta(days=1)
TEST_DT_2 = START_TIME - timedelta(hours=12)


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

x = {}
for f_name in TEST_FILE_LIST[:8]:
    x[f_name] = TEST_DT_1
for f_name in TEST_FILE_LIST[8:]:
    x[f_name] = TEST_DT_2


@patch('neossat2caom2.composable.HttpDataSourceRunnerMeta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutesRunnerMeta.do_one')
def test_run_state(run_mock, access_mock, data_source_mock, test_config, tmp_path, change_test_dir):

    class MockDS(HttpDataSourceRunnerMeta):

        def _descend_html_hierarchy(self, _):
            self._todo_list = x

    data_source_mock.side_effect = MockDS
    access_mock.return_value = 'https://localhost'

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.proxy_file_name = 'textproxy.pem'
    test_config.interval = 200
    test_config.logging_level = 'INFO'

    test_config.write_to_file(test_config)
    mc.State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], TEST_START_TIME)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    run_mock.return_value = (0, None)
    test_result = composable._run_state()
    assert test_result == 0, 'expected successful test result'
    assert run_mock.called, 'should be called'
    args, _ = run_mock.call_args
    test_storage = args[0]
    assert isinstance(test_storage, NEOSSatName), type(test_storage)
    assert re.match('^NEOS_SCI.*\\.fits$', test_storage.file_name) is not None
    assert run_mock.call_count == 10, 'wrong call count'
    test_state_end = mc.State(test_config.state_fqn, test_config.time_zone)
    test_end_bookmark = test_state_end.get_bookmark(test_config.data_sources[0])
    assert TEST_DT_2 == test_end_bookmark, f'wrong end time max {TEST_DT_2} end {test_end_bookmark}'


@patch('neossat2caom2.composable.HttpDataSourceRunnerMeta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutesRunnerMeta.do_one')
def test_run_state_zero_records(run_mock, access_mock, data_source_mock, test_config, tmp_path, change_test_dir):
    # this tests end conditions when there is no work at the data source (i.e. CSA http site),
    # in this case, the value in the state.yml file should not change, since the value in
    # the state.yml file is the timestamp of the last record processed

    mock_called = False

    class MockDSNoRecords(HttpDataSourceRunnerMeta):

        def __init__(self, config, start_key, html_filters, session, storage_name_ctor):
            super().__init__(config, start_key, html_filters, session, storage_name_ctor)

        def _descend_html_hierarchy(self, ignore_node):
            self._todo_list = {}
            nonlocal mock_called
            mock_called = True

    data_source_mock.side_effect = MockDSNoRecords

    access_mock.return_value = 'https://localhost'
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.proxy_file_name = 'textproxy.pem'
    test_config.interval = 200
    test_config.logging_level = 'INFO'

    test_config.write_to_file(test_config)
    mc.State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], TEST_START_TIME)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    run_mock.return_value = (0, None)
    test_result = composable._run_state()
    assert test_result == 0, 'expected successful test result'
    assert mock_called, 'initialize should be called'
    assert not run_mock.called, 'no records, should not be called'
    test_state_end = mc.State(test_config.state_fqn, test_config.time_zone)
    test_end_bookmark = test_state_end.get_bookmark(test_config.data_sources[0])
    assert test_end_bookmark == TEST_START_TIME, f'wrong end time max {TEST_START_TIME} end {test_end_bookmark}'


@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutesRunnerMeta.do_one')
def test_run_by_file(do_one_mock, clients_mock, test_data_dir, test_config, tmp_path, change_test_dir):
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.write_to_file(test_config)
    do_one_mock.return_value = (0, None)
    with open(test_config.work_fqn, 'w') as f_out:
        f_out.write(
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930_clean.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005240_clean.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005240.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005550_clean.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005550.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005900_clean.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268005900.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268010210_clean.fits\n'
            '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268010210.fits\n'
        )
    clients_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    clients_mock.return_value.metadata_client.create.side_effect = Mock()
    clients_mock.return_value.metadata_client.update.side_effect = _mock_repo_update
    clients_mock.return_value.data_client.info.side_effect = _mock_get_file_info
    test_result = composable._run()
    assert test_result == 0, 'wrong result'
    # ClientVisit executor only with the test configuration
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 10, 'wrong number of mock calls'


@patch('caom2pipe.execute_composable.get_local_file_headers')
@patch('caom2pipe.execute_composable.get_local_file_info')
@patch('neossat2caom2.fits2caom2_augmentation.visit')
@patch('caom2pipe.astro_composable.check_fitsverify')
@patch('caom2pipe.manage_composable.http_get')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_by_file_store_ingest_modify(
    clients_mock, 
    http_get_mock, 
    fits_verify_mock, 
    visit_mock, 
    file_info_mock, 
    file_header_mock, 
    test_data_dir, 
    test_config, 
    tmp_path, 
    change_test_dir,
):
    test_file_uri = 'cadc:NEOSSAT/NEOS_SCI_2019268004930_clean.fits'
    file_header_mock.return_value = []
    file_info_mock.return_value = FileInfo(
        id=test_file_uri,
        file_type='application/fits', 
    )
    visit_mock.side_effect = _mock_visit_wd
    fits_verify_mock.return_value = True
    http_get_mock.side_effect = _mock_http_get

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST, mc.TaskType.MODIFY]
    test_config.use_local_files = False
    test_config.logging_level = 'INFO'
    test_config.write_to_file(test_config)
    # do_one_mock.return_value = (0, None)
    with open(test_config.work_fqn, 'w') as f_out:
        f_out.write(
            'https://data.asc-csa.gc.ca/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930_clean.fits\n'
        )
    clients_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    clients_mock.return_value.metadata_client.create.side_effect = Mock()
    clients_mock.return_value.metadata_client.update.side_effect = _mock_repo_update
    clients_mock.return_value.data_client.info.side_effect = _mock_get_file_info
    test_result = composable._run()
    assert test_result == 0, 'wrong result'
    # assert do_one_mock.called, 'expect do_one call'
    # assert do_one_mock.call_count == 1, 'wrong number of mock calls'
    assert clients_mock.return_value.data_client.put.called, 'put call'
    clients_mock.return_value.data_client.put.assert_called_with(
        f'{tmp_path}/2019268004930', test_file_uri
    ), 'put call args'
    assert clients_mock.return_value.metadata_client.read.called, 'read call'
    clients_mock.return_value.metadata_client.read.assert_called_with('NEOSSAT', '2019268004930'), 'read call'
    assert clients_mock.return_value.metadata_client.update.called, 'modify call'
    clients_mock.return_value.metadata_client.update.assert_called_with(ANY), 'modify call'


@patch('caom2pipe.execute_composable.get_local_file_info')
@patch('caom2pipe.execute_composable.get_local_file_headers')
def test_store(headers_mock, file_info_mock, test_data_dir, test_config, tmp_path, change_test_dir):
    file_info_mock.side_effect = _mock_get_file_info
    headers_mock.side_effect = make_headers_from_file(f'{test_data_dir}/NEOS_SCI_2015347015200_clean.fits.header')
    test_config.change_working_directory(tmp_path.as_posix())
    test_fqn = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2019/268/NEOS_SCI_2019268004930_clean.fits'
    test_storage_name = NEOSSatName([test_fqn])
    transferrer = Mock()
    clients_mock = Mock()
    test_reporter = mc.ExecutionReporter2(test_config)
    test_subject = ec.StoreRunnerMeta(clients_mock, test_config, test_reporter, transferrer)
    test_subject.execute({'storage_name': test_storage_name})
    assert clients_mock.data_client.put.called, 'expect a put call'
    clients_mock.data_client.put.assert_called_with(
        f'{tmp_path}/2019268004930', 'cadc:NEOSSAT/NEOS_SCI_2019268004930_clean.fits'
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    transferrer.get.assert_called_with(
        test_fqn, f'{tmp_path}/2019268004930/NEOS_SCI_2019268004930_clean.fits'
    ), 'wrong get args'


def test_scrape_modify_compressed(test_config, tmp_path):
    # test the scrape + modify test case, with compressed files
    # will also execute preview generation
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.logging_level = 'INFO'
    test_config.use_local_files = True
    test_config.log_to_file = True
    test_config.task_types = [mc.TaskType.SCRAPE, mc.TaskType.MODIFY]
    test_config.data_sources = [f'{TEST_DATA_DIR}/../../../int_test']
    test_config.data_source_extensions = ['.fits.gz']
    orig_dir = os.getcwd()
    try:
        os.chdir(tmp_path)
        test_config.write_to_file(test_config)
        test_result = composable._run()
        assert test_result == 0, 'expect success'
        test_summary = mc.ExecutionSummary.read_report_file(test_config.report_fqn)
        assert test_summary.entries == 2, 'wrong entries count'
        assert test_summary.success == 2, 'wrong success count'
    except Exception as e:
        os.chdir(orig_dir)
        assert False, f'expect no exceptions {e}'


def _mock_repo_read(collection, obs_id):
    return SimpleObservation(collection=collection, observation_id=obs_id)


def _mock_visit_wd(obs, **kwargs):
    return _mock_repo_read('NEOSSat', 'test_obs')


def _mock_repo_update(ignore1):
    return None


def _mock_get_file_info(uri):
    if '_prev' in uri:
        return {'type': 'image/jpeg'}
    else:
        return {'type': 'application/fits'}


def _mock_http_get(url, local_fqn):
    pass
