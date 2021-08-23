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

from mock import patch

from cadcutils import exceptions
from cadcdata import FileInfo
from neossat2caom2 import main_app, NEOSSatName, APPLICATION
from neossat2caom2 import COLLECTION
from caom2pipe import manage_composable as mc

import os
import sys

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'main_app.py')


LOOKUP = {
    '2019213173800': [
        'NEOS_SCI_2019213173800',
        'NEOS_SCI_2019213173800_cor',
        'NEOS_SCI_2019213173800_cord',
    ],
    '2019258000140': [
        'NEOS_SCI_2019258000140',
        'NEOS_SCI_2019258000140_clean',
    ],
    '2019259111450': [
        'NEOS_SCI_2019259111450',
        'NEOS_SCI_2019259111450_clean',
    ],
    '2019213174531': [
        'NEOS_SCI_2019213174531',
        'NEOS_SCI_2019213174531_cor',
        'NEOS_SCI_2019213174531_cord',
    ],
    '2019213215700': [
        'NEOS_SCI_2019213215700',
        'NEOS_SCI_2019213215700_cor',
        'NEOS_SCI_2019213215700_cord',
    ],
    # dark
    '2019267234420': ['NEOS_SCI_2019267234420_clean'],
    # no RA, DEC keywords
    '2015347015200': ['NEOS_SCI_2015347015200_clean'],
    # PI Name
    '2020255152013': ['NEOS_SCI_2020255152013_clean'],
}


def pytest_generate_tests(metafunc):
    obs_id_list = []
    for ii in LOOKUP:
        obs_id_list.append(ii)
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2utils.data_util.StorageClientWrapper')
def test_main_app(data_client_mock, test_name):
    basename = os.path.basename(test_name)
    neos_name = NEOSSatName(file_name=basename, entry=basename)
    output_file = '{}/{}.actual.xml'.format(TEST_DATA_DIR, basename)
    obs_path = '{}/{}'.format(
        TEST_DATA_DIR, '{}.expected.xml'.format(neos_name.obs_id)
    )

    def cadcinfo_mock(uri):
        return FileInfo(id=uri, file_type='application/fits')

    data_client_mock.return_value.info.side_effect = cadcinfo_mock
    sys.argv = (
        '{} --no_validate --local {} --observation {} {} -o {} '
        '--plugin {} --module {} --lineage {}'.format(
            APPLICATION,
            _get_local(test_name),
            COLLECTION,
            test_name,
            output_file,
            PLUGIN,
            PLUGIN,
            _get_lineage(test_name),
        )
    ).split()
    print(sys.argv)
    main_app.to_caom2()

    compare_result = mc.compare_observations(output_file, obs_path)
    if compare_result is not None:
        raise AssertionError(compare_result)
    # assert False  # cause I want to see logging messages


def _get_lineage(obs_id):
    result = ''
    for ii in LOOKUP[obs_id]:
        product_id = NEOSSatName.extract_product_id(ii)
        fits = mc.get_lineage(
            COLLECTION, product_id, '{}.fits'.format(ii), 'cadc'
        )
        result = '{} {}'.format(result, fits)
    return result


def _get_local(obs_id):
    result = ''
    for ii in LOOKUP[obs_id]:
        result = '{} {}/{}.fits.header'.format(result, TEST_DATA_DIR, ii)
    return result
