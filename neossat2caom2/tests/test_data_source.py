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

from datetime import datetime, timezone
from os.path import basename, dirname, join, realpath

from caom2pipe.manage_composable import Config, ExecutionReporter, make_time_tz, State
from neossat2caom2.composable import NEOS_BOOKMARK
from neossat2caom2.data_source import CSADataSource
from unittest.mock import Mock, patch

THIS_DIR = dirname(realpath(__file__))
TEST_DATA_DIR = join(THIS_DIR, 'data')

TOP_PAGE = f'{TEST_DATA_DIR}/top_page.html'
YEAR_PAGE = f'{TEST_DATA_DIR}/year_page.html'
DAY_PAGE = f'{TEST_DATA_DIR}/day_page.html'
TEST_START_TIME_STR = '2022-02-01T13:57:00'

test_time_zone = timezone.utc


def test_parse_functions():
    # test _parse_* functions in the DataSource specializations
    test_config = Config()
    test_config.data_source = ['https://data.asc-csa.gc.ca/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/']
    test_config.data_source_extensions = ['.fits', '.fits.gz']
    test_config.state_file_name = 'parse_test_state.yml'
    test_config.state_fqn = join(TEST_DATA_DIR, 'parse_test_state.yml')
    test_subject = CSADataSource(test_config)
    assert test_subject is not None, 'ctor failure'
    test_subject._start_time = make_time_tz(TEST_START_TIME_STR, test_time_zone)

    with open(TOP_PAGE) as f:
        test_content = f.read()
        test_result = test_subject._parse_top_page(test_config.data_source[0], test_content)
        assert test_result is not None, 'expected a top page result'
        assert len(test_result) == 2, 'wrong number of top page results'
        top_first_answer = next(iter(sorted(test_result.items())))
        assert len(top_first_answer) == 2, 'wrong number of top page results'
        assert top_first_answer[0] == datetime(2022, 3, 1, 3, 19, tzinfo=test_time_zone)
        assert top_first_answer[1][0] == f'{test_config.data_source[0]}2021'

    with open(YEAR_PAGE) as f:
        test_content = f.read()
        test_result = test_subject._parse_year_page(top_first_answer[1][0], test_content)
        assert test_result is not None, 'expect a result'
        assert len(test_result) == 4, 'wrong number of year page results'
        year_first_answer = next(iter(sorted(test_result.items())))
        assert year_first_answer[0] == datetime(2022, 3, 1, 13, 58, tzinfo=test_time_zone), 'wrong year page timestamp'
        assert year_first_answer[1][0] == f'{test_config.data_source[0]}2021/001', 'wrong year page url'

    with open(DAY_PAGE) as f:
        test_content = f.read()
        test_result = test_subject._parse_day_page(year_first_answer[1][0], test_content)
        assert test_result is not None, 'expected a day page result'
        assert len(test_result) == 3, 'wrong number of day page results'
        first_answer = next(iter(sorted(test_result.items())))
        assert len(first_answer) == 2, 'wrong number of day page results'
        assert first_answer[0] == datetime(2022, 3, 1, 13, 56, tzinfo=test_time_zone)
        assert first_answer[1][0] == f'{test_config.data_source[0]}2021/001/NEOS_SCI_2022001030508.fits', 'wrong content'


@patch('neossat2caom2.data_source.query_endpoint_session')
def test_incremental_source(query_endpoint_mock, test_config, tmpdir):
    query_endpoint_mock.side_effect = _query_endpoint
    test_config.change_working_directory(tmpdir)
    test_config.write_to_file(test_config)
    State.write_bookmark(test_config.state_fqn, NEOS_BOOKMARK, TEST_START_TIME_STR)

    test_start_time = make_time_tz(TEST_START_TIME_STR, test_time_zone)
    test_subject = CSADataSource(test_config, test_start_time)
    assert test_subject is not None, 'ctor failure'
    test_reporter = ExecutionReporter(test_config, observable=Mock(), application='DEFAULT')
    test_subject.reporter = test_reporter
    test_result = test_subject.get_time_box_work(
        test_start_time, datetime.fromtimestamp(1656143080, tz=test_time_zone)
    )
    assert test_result is not None, 'expected dict result'
    assert len(test_result) == 108, 'wrong size results'
    temp = sorted([ii.entry_name for ii in test_result])
    assert (
        temp[0]
        == 'https://localhost:8888/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2021/001/'
           'NEOS_SCI_2022001030508.fits'
    ), 'wrong result'
    assert test_subject.max_time is not None, 'expected date result'
    assert test_subject.max_time == datetime(2022, 8, 22, 15, 30, tzinfo=test_time_zone), 'wrong date result'
    assert test_reporter._summary._entries_sum == 108, f'wrong entries {test_reporter._summary.report()}'


def _query_endpoint(url, session):

    def _close():
        pass

    result = type('response', (), {})
    result.text = None
    result.close = _close

    base_name = basename(url)

    if url == 'https://localhost:8888/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/':
        with open(TOP_PAGE, 'r') as f:
            result.text = f.read()
    elif base_name in ['2021', '2022']:
        with open(YEAR_PAGE) as f:
            result.text = f.read()
    elif base_name in ['001', '002', '310', '311', '312', '313']:
        with open(DAY_PAGE) as f:
            result.text = f.read()
    else:
        raise Exception(f'wut? {url}')
    return result
