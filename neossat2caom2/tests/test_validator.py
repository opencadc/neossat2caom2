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
from caom2pipe.validator_composable import VALIDATE_OUTPUT

from neossat2caom2 import validator

from mock import patch, Mock


@patch('neossat2caom2.data_source.CSADataSource.get_work')
@patch('cadcutils.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_validator(caps_mock, post_mock, get_work_mock, test_config, tmpdir):
    caps_mock.return_value = 'https://localhost:8888'
    response = Mock()
    response.status_code = 200
    y = [
        b'uri\tcontentLastModified\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700_cord.fits\t2022-01-01 12:00:00+00:00\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700_cor.fits\t2022-01-01 12:00:00+00:00\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700.fits\t2022-01-01 12:00:00+00:00\n'
    ]

    x = [b'uri\tcontentLastModified\n']

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

    get_work_mock.return_value = {}
    test_config.change_working_directory(tmpdir)
    test_config.proxy_file_name = 'test_proxy.pem'
    test_config.logging_level = 'INFO'
    mc.Config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('proxy content')

    test_subject = validator.NeossatValidator()
    test_listing_fqn = f'{test_config.working_directory}/not_at_NEOSSAT.txt'
    test_source, test_meta, test_data = test_subject.validate()
    assert test_source is not None, 'expected source result'
    assert test_meta is not None, 'expected destination result'
    assert len(test_source) == 0, 'wrong number of source results'
    assert len(test_meta) == 3, 'wrong # of destination results'
    test_meta.f_name.isin(['NEOS_SCI_2019213215700_cor.fits']), 'wrong destination content'
    assert len(test_data) == 0, 'wrong number of data results'
    assert os.path.exists(test_listing_fqn), 'should create file record'
    test_subject.write_todo()
    assert not os.path.exists(test_subject._config.work_fqn), 'no records, should not create file record'
