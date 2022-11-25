# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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

import os
import shutil

from caom2 import ChecksumURI
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc

from neossat2caom2 import preview_augmentation, NEOSSatName
import test_main_app


def test_preview_augmentation(test_config, tmpdir):
    test_config.data_sources = ['/test_files']
    test_config.change_working_directory(tmpdir)
    test_obs_fqn = f'{test_main_app.TEST_DATA_DIR}/2019213215700.expected.xml'
    test_obs = mc.read_obs_from_file(test_obs_fqn)
    assert test_obs is not None, 'expect an input'
    assert len(test_obs.planes) == 3, 'expect 3 planes'
    for product_id in ['raw', 'cor', 'cord']:
        assert (
            len(test_obs.planes[product_id].artifacts) == 1
        ), 'wrong artifact pre-condition count'

    test_file_names = [
        'NEOS_SCI_2019213215700_cor.fits',
        'NEOS_SCI_2019213215700.fits',
        'NEOS_SCI_2019213215700_cord.fits',
    ]

    test_builder = nbc.GuessingBuilder(NEOSSatName)
    for f_name in test_file_names:
        test_fqn = f'/test_files/{f_name}'
        if not os.path.exists(test_fqn):
            shutil.copy(f'{test_main_app.TEST_DATA_DIR}/{f_name}', test_fqn)
        test_storage_name = test_builder.build(test_fqn)
        if os.path.exists(f'/test_files/{test_storage_name.prev}'):
            os.unlink(f'/test_files/{test_storage_name.prev}')
        if os.path.exists(f'/test_files/{test_storage_name.thumb}'):
            os.unlink(f'/test_files/{test_storage_name.thumb}')
        kwargs = {
            'working_directory': '/test_files',
            'storage_name': test_storage_name,
        }
        test_obs = preview_augmentation.visit(test_obs, **kwargs)
        assert test_obs is not None, 'expect a result'
        assert os.path.exists(f'/test_files/{test_storage_name.prev}')
        assert os.path.exists(f'/test_files/{test_storage_name.thumb}')

    for product_id in ['raw', 'cor', 'cord']:
        assert (
                len(test_obs.planes[product_id].artifacts) == 3
        ), 'wrong artifact post-condition count'
        if product_id == 'raw':
            preva = 'cadc:NEOSSAT/2019213215700_raw_prev.png'
            thumba = 'cadc:NEOSSAT/2019213215700_raw_prev_256.png'
            assert (
                test_obs.planes[product_id].artifacts[preva].content_checksum
                == ChecksumURI('md5:2ecc93cffef79f0068eb6305f232192c')
            ), 'prev checksum failure'
            assert (
                test_obs.planes[product_id].artifacts[thumba].content_checksum
                == ChecksumURI('md5:f6db75d585a310f13d5f3d442d2e9ec5')
            ), 'thumb checksum failure'
