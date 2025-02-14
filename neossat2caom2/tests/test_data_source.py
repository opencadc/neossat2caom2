# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
#  : 4 $
#
# ***********************************************************************
#

from datetime import datetime
from os.path import basename, dirname, join, realpath

from caom2pipe.html_data_source import HttpDataSource
from caom2pipe.manage_composable import ExecutionReporter, make_datetime, State
from neossat2caom2.data_source import NeossatPagesTemplate
from unittest.mock import Mock, patch

THIS_DIR = dirname(realpath(__file__))
TEST_DATA_DIR = join(THIS_DIR, 'data')

TOP_PAGE = f'{TEST_DATA_DIR}/top_page.html'
YEAR_PAGE = f'{TEST_DATA_DIR}/year_page.html'
DAY_PAGE = f'{TEST_DATA_DIR}/day_page.html'
TEST_START_TIME_STR = '2022-02-01T13:57:00'


@patch('caom2pipe.html_data_source.query_endpoint_session')
def test_incremental_source(query_endpoint_mock, test_config, tmpdir):
    query_endpoint_mock.side_effect = _query_endpoint
    test_config.data_sources = ['https://localhost:8888/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/']
    test_config.change_working_directory(tmpdir)
    test_start_time = make_datetime(TEST_START_TIME_STR)
    State.write_bookmark(test_config.state_fqn, test_config.data_sources[0], test_start_time)

    test_html_filters = NeossatPagesTemplate(test_config)
    session_mock = Mock()
    test_subject = HttpDataSource(test_config, test_config.data_sources[0], test_html_filters, session_mock)
    assert test_subject is not None, 'ctor failure'

    test_subject.initialize_start_dt()
    assert test_subject.start_dt == datetime(2022, 2, 1, 13, 57), 'wrong start time'
    test_subject.initialize_end_dt()
    test_reporter = ExecutionReporter(test_config, observable=Mock())
    test_subject.reporter = test_reporter
    test_result = test_subject.get_time_box_work(test_start_time, datetime.fromtimestamp(1656143080))
    assert test_result is not None, 'expected dict result'
    # 63 == (10 file names - 1 too old file name) * 7 directory listings
    assert len(test_result) == 63, 'wrong size results'
    temp = sorted([ii.entry_name for ii in test_result])
    assert (
        temp[0] == 'https://localhost:8888/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2017/313/'
        'NEOS_SCI_2022001030508.fits'
    ), 'wrong result'
    assert test_subject.end_dt is not None, 'expected date result'
    assert test_subject.end_dt == datetime(2022, 8, 22, 15, 30), 'wrong date result'
    assert test_reporter._summary._entries_sum == 63, f'wrong entries {test_reporter._summary.report()}'


def _query_endpoint(url, ignore_session):
    result = type('response', (), {})
    result.text = None
    result.close = lambda: None
    result.raise_for_status = lambda: None

    base_name = basename(url.rsplit('/', 1)[0])

    if url == 'https://localhost:8888/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/':
        with open(TOP_PAGE, 'r') as f:
            result.text = f.read()
    elif base_name in ['2017', '2018', '2019', '2020', '2021', '2022', 'NESS']:
        with open(YEAR_PAGE) as f:
            result.text = f.read()
    elif base_name in ['001', '002', '310', '311', '312']:
        result.text = ''
    elif base_name == '313':
        with open(DAY_PAGE) as f:
            result.text = f.read()
    else:
        raise Exception(f'wut? {base_name} {url}')
    return result
