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
import pytest

from mock import patch

from caom2 import ChecksumURI
from neossat2caom2 import preview_augmentation, NEOSSatName
from caom2pipe import manage_composable as mc


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_OBS = '2019213215700'
TEST_FILES = ['NEOS_SCI_2019213215700.fits',
              'NEOS_SCI_2019213215700_cor.fits',
              'NEOS_SCI_2019213215700_cord.fits']


def test_preview_aug_visit():
    with pytest.raises(mc.CadcException):
        preview_augmentation.visit(None)


def test_preview_augment_plane():
    for f_name in TEST_FILES:
        neoss_name = NEOSSatName(file_name=f_name)
        preview = os.path.join(TEST_DATA_DIR, neoss_name.prev)
        thumb = os.path.join(TEST_DATA_DIR, neoss_name.thumb)
        if os.path.exists(preview):
            os.remove(preview)
        if os.path.exists(thumb):
            os.remove(thumb)
        test_fqn = os.path.join(TEST_DATA_DIR, '{}.fits.xml'.format(TEST_OBS))
        test_obs = mc.read_obs_from_file(test_fqn)
        assert len(test_obs.planes[neoss_name.product_id].artifacts) == 1

        test_config = mc.Config()
        test_config.observe_execution = True
        test_metrics = mc.Metrics(test_config)
        test_observable = mc.Observable(rejected=None,
                                        metrics=test_metrics)

        test_kwargs = {'working_directory': TEST_DATA_DIR,
                       'cadc_client': None,
                       'observable': test_observable,
                       'stream': test_config.stream,
                       'science_file': f_name}
        with patch('neossat2caom2.preview_augmentation._store_smalls'):
            test_result = preview_augmentation.visit(test_obs, **test_kwargs)
            assert test_result is not None, 'expected a visit return value'
            assert test_result['artifacts'] == 2
            assert len(test_obs.planes[neoss_name.product_id].artifacts) == 3

            if f_name == 'NEOS_SCI_2019213215700.fits':
                preva = 'ad:NEOSS/2019213215700_raw_prev.png'
                thumba = 'ad:NEOSS/2019213215700_raw_prev_256.png'
                assert os.path.exists(preview)
                assert os.path.exists(thumb)
                assert test_obs.planes[neoss_name.product_id].\
                    artifacts[preva].content_checksum == \
                    ChecksumURI('md5:cbd42f0751d412799a64cfc5792bf08d'), \
                    'prev checksum failure'
                assert test_obs.planes[neoss_name.product_id].\
                    artifacts[thumba].content_checksum == \
                    ChecksumURI('md5:fcfee77f6dcd56f1b1fc3c91450b7393'), \
                    'thumb checksum failure'
