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

from caom2pipe import manage_composable as mc

from neossat2caom2 import validator, scrape
from tempfile import TemporaryDirectory

from mock import patch, Mock
import test_scrape


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('neossat2caom2.scrape.FTPHost')
def test_validator(ftp_mock, caps_mock, post_mock, test_config):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    y = [
        b'uri\n'
        b'ad:NEOSS/NEOS_SCI_2019213215700_cord.fits\n'
        b'ad:NEOSS/NEOS_SCI_2019213215700_cor.fits\n'
        b'ad:NEOSS/NEOS_SCI_2019213215700.fits\n'
    ]

    x = [b'ingestDate,fileName\n']

    global count
    count = 0

    def _mock_shit(chunk_size):
        global count
        if count == 0:
            count = 1
            return y
        else:
            return x

    response.iter_content.side_effect = _mock_shit
    post_mock.return_value.__enter__.return_value = response

    test_scrape._make_test_dirs()
    ftp_mock.return_value.__enter__.return_value.listdir.side_effect = (
        test_scrape._list_dirs
    )
    ftp_mock.return_value.__enter__.return_value.stat.side_effect = (
        test_scrape._entry_stats
    )

    orig_cwd = os.getcwd()
    try:
        with TemporaryDirectory() as tmp_dir_name:
            os.chdir(tmp_dir_name)
            test_config.change_working_directory(tmp_dir_name)
            test_config.proxy_file_name = 'test_proxy.pem'
            test_config.logging_level = 'INFO'
            mc.Config.write_to_file(test_config)
            with open(test_config.proxy_fqn, 'w') as f:
                f.write('proxy content')

            test_subject = validator.NeossatValidator()
            test_listing_fqn = f'{test_config.working_directory}/{mc.VALIDATE_OUTPUT}'
            test_source, test_meta, test_data = test_subject.validate()
            assert test_source is not None, 'expected source result'
            assert test_meta is not None, 'expected destination result'
            assert len(test_source) == 3, 'wrong number of source results'
            assert (
                'NEOS_SCI_2019213215700_clean.fits' in test_source
            ), 'wrong source content'
            assert len(test_meta) == 1, 'wrong # of destination results'
            assert (
                'NEOS_SCI_2019213215700_cor.fits' in test_meta
            ), 'wrong destination content'
            assert os.path.exists(test_listing_fqn), 'should create file record'

            test_subject.write_todo()
            assert os.path.exists(
                test_subject._config.work_fqn
            ), 'should create file record'
            with open(test_subject._config.work_fqn, 'r') as f:
                content = f.readlines()
            content_sorted = sorted(content)
            assert (
                content_sorted[0] == '/users/OpenData_DonneesOuvertes/pub/'
                'NEOSSAT/ASTRO/M32/NEOS_SCI_2017213215701.fits\n'
            ), 'unexpected content'

            # does the cached list work too?
            test_cache = test_subject.read_from_source()
            assert test_cache is not None, 'expected cached source result'
            assert next(iter(test_cache)) == 'NEOS_SCI_2017213215701.fits', 'wrong cached result'
    finally:
        os.chdir(orig_cwd)
