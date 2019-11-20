# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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
import stat
import shutil

from mock import patch, Mock

from caom2pipe import manage_composable as mc
from neossat2caom2 import scrape
import test_main_app

TEST_DIRS = [
    '/tmp/astro',
    '/tmp/astro/2017',
    '/tmp/astro/2018',
    '/tmp/astro/2019',
    '/tmp/astro/2017/125',
    '/tmp/astro/2017/127',
    '/tmp/astro/2017/125/dark',
    '/tmp/astro/2017/125/dark/fine_point',
    '/tmp/astro/2017/125/m31',
    '/tmp/astro/2017/125/m31/reacquire',
]

TEST_END_TIME = 15704859000

MOCK_DIR = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/M32'
EXISTING_MOCK_DIR = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/M33'


@patch('neossat2caom2.scrape.FTPHost')
def test_append_todo(ftp_mock):
    _make_test_dirs()
    ftp_mock.return_value.__enter__.return_value.listdir.\
        side_effect = _list_dirs
    ftp_mock.return_value.__enter__.return_value.stat. \
        side_effect = _entry_stats
    test_start = TEST_END_TIME - 1000
    test_result = scrape._append_todo(
        test_start, '/usr/src/app', 'localhost', '/tmp/astro', {}, {})
    assert test_result is not None, 'expected result'
    assert len(test_result) == 14, 'wrong length of all the entries'
    test_todo_list, test_max = scrape._remove_dir_names(test_result, test_start)
    assert test_todo_list is not None, 'expect a todo list'
    assert test_max is not None, 'expect a max time'
    assert test_max == TEST_END_TIME, 'wrong max time'
    assert len(test_todo_list) == 5, 'wrong length of file entries'
    assert '/tmp/astro/2017/125/dark/fine_point/a.fits' in test_todo_list, \
        'missing leafiest entry'
    assert '/tmp/astro/2017/127/e.fits' in test_todo_list, \
        'missing less leafy entry'


@patch('neossat2caom2.scrape.FTPHost')
def test_list_for_validate(ftp_mock):
    # put some test appending files in place - these files indicate
    # a 'No Exceptions' ending to list_for_validate occurred last
    # time
    cache_fqn = os.path.join(test_main_app.TEST_DATA_DIR,
                         scrape.NEOSSAT_CACHE)
    if os.path.exists(cache_fqn):
        os.unlink(cache_fqn)
    source_list_fqn = os.path.join(
        test_main_app.TEST_DATA_DIR, scrape.NEOSSAT_SOURCE_LIST)
    source_fqn = os.path.join(test_main_app.TEST_DATA_DIR,
                              'test_source_listing.yml')
    shutil.copy(source_fqn, source_list_fqn)
    _execute_and_check_list_for_validate(ftp_mock, source_list_fqn, 8, 2)


@patch('neossat2caom2.scrape.FTPHost')
def test_list_for_validate_exceptional_ending(ftp_mock):
    # put some test appending files in place - these files indicate
    # an FTPOSError ending to list_for_validate occurred last
    # time
    source_list_fqn = os.path.join(
        test_main_app.TEST_DATA_DIR, scrape.NEOSSAT_SOURCE_LIST)
    if os.path.exists(source_list_fqn):
        os.unlink(source_list_fqn)
    cache_fqn = os.path.join(test_main_app.TEST_DATA_DIR,
                             scrape.NEOSSAT_CACHE)
    source_fqn = os.path.join(test_main_app.TEST_DATA_DIR,
                              'test_cache_listing.csv')
    shutil.copy(source_fqn, cache_fqn)
    _execute_and_check_list_for_validate(ftp_mock, source_list_fqn, 5, 14)


class StatMock(object):
    def __init__(self, mtime, mode):
        self.st_mtime = mtime
        self.st_mode = mode


def _execute_and_check_list_for_validate(ftp_mock, source_list_fqn,
                                         result_count, cache_count):
    source_dir_fqn = os.path.join(
        test_main_app.TEST_DATA_DIR, scrape.NEOSSAT_DIR_LIST)
    source_fqn = os.path.join(test_main_app.TEST_DATA_DIR,
                              'test_source_dir_listing.csv')
    shutil.copy(source_fqn, source_dir_fqn)

    ftp_mock.return_value.__enter__.return_value.listdir. \
        side_effect = _list_dirs
    ftp_mock.return_value.__enter__.return_value.stat. \
        side_effect = _entry_stats
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        test_config = mc.Config()
        test_config.get_executors()
        scrape.list_for_validate(test_config)

        result = mc.read_as_yaml(source_list_fqn)
        assert result is not None, 'expect a file record'
        assert len(result) == result_count, 'wrong number of entries'
        assert f'{MOCK_DIR}/NEOS_SCI_2017213215701_cord.fits' in result, \
            'wrong content'

        cache_result = scrape._read_cache(test_config.working_directory)
        assert cache_result is not None, 'expected return value'
        assert len(cache_result) == cache_count, \
            'wrong number of cached entries'
        assert f'{MOCK_DIR}/NEOS_SCI_2017213215701.fits' in cache_result, \
            'wrong content'
    finally:
        os.getcwd = getcwd_orig


def _entry_stats(fqn):
    dir_mode = stat.S_IFDIR
    file_mode = stat.S_IFREG
    if fqn in TEST_DIRS or fqn == MOCK_DIR or fqn == EXISTING_MOCK_DIR:
        result = StatMock(mtime=TEST_END_TIME, mode=dir_mode)
    else:
        result = StatMock(mtime=TEST_END_TIME, mode=file_mode)
    return result


def _list_dirs(dir_name):
    if dir_name == '/tmp/astro':
        return ['2017', '2018', '2019']
    elif dir_name == '/tmp/astro/2017':
        return ['125', '127']
    elif dir_name == '/tmp/astro/2017/125':
        return ['dark', 'm31']
    elif dir_name == '/tmp/astro/2017/125/dark':
        return ['fine_point']
    elif dir_name == '/tmp/astro/2017/125/m31':
        return ['reacquire']
    elif dir_name == '/tmp/astro/2017/125/dark/fine_point':
        return ['a.fits', 'b.fits']
    elif dir_name == '/tmp/astro/2017/125/m31/reacquire':
        return ['c.fits', 'd.fits']
    elif dir_name == '/tmp/astro/2017/127':
        return ['e.fits']
    elif dir_name == '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO':
        return ['NEOS_SCI_2019213215700_cord.fits',
                'NEOS_SCI_2019213215700.fits',
                'NEOS_SCI_2019213215700_clean.fits',
                'M32',  # new content
                'M33']  # already in list
    elif dir_name == MOCK_DIR:
        return ['NEOS_SCI_2017213215701_cord.fits',
                'NEOS_SCI_2017213215701.fits']
    else:
        return []


def _make_test_dirs():
    if not os.path.exists('/tmp/astro'):
        for dir_name in TEST_DIRS:
            os.mkdir(dir_name)
        with open('/tmp/astro/2017/125/dark/fine_point/a.fits', 'w') as f:
            f.write('abc')
        with open('/tmp/astro/2017/125/dark/fine_point/b.fits', 'w') as f:
            f.write('abc')
        with open('/tmp/astro/2017/125/m31/reacquire/c.fits', 'w') as f:
            f.write('abc')
        with open('/tmp/astro/2017/125/m31/reacquire/d.fits', 'w') as f:
            f.write('abc')
        with open('/tmp/astro/2017/127/e.fits', 'w') as f:
            f.write('abc')
