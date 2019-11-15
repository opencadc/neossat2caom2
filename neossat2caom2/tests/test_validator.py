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

from mock import patch, Mock
import test_main_app, test_scrape


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('neossat2caom2.scrape.FTPHost')
def test_validator(ftp_mock, caps_mock, tap_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = \
        [b'<?xml version="1.0" encoding="UTF-8"?>\n'
         b'<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3" '
         b'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3">\n'
         b'<RESOURCE type="results">\n'
         b'<INFO name="QUERY_STATUS" value="OK" />\n'
         b'<INFO name="QUERY_TIMESTAMP" value="2019-11-14T16:26:46.274" />\n'
         b'<INFO name="QUERY" value="SELECT distinct A.uri&#xA;FROM '
         b'caom2.Observation as O&#xA;JOIN caom2.Plane as P on O.obsID = '
         b'P.obsID&#xA;JOIN caom2.Artifact as A on P.planeID = A.planeID&#xA;'
         b'WHERE O.collection = \'NEOSSAT\'&#xA;AND A.uri like '
         b'\'%2019213215700%\'" />\n'
         b'<TABLE>\n'
         b'<FIELD name="uri" datatype="char" arraysize="*" '
         b'utype="caom2:Artifact.uri" xtype="uri">\n'
         b'<DESCRIPTION>external URI for the physical artifact</DESCRIPTION>\n'
         b'</FIELD>\n'
         b'<DATA>\n'
         b'<TABLEDATA>\n'
         b'<TR>\n'
         b'<TD>ad:NEOSS/NEOS_SCI_2019213215700_cord.fits</TD>\n'
         b'</TR>\n'
         b'<TR>\n'
         b'<TD>ad:NEOSS/NEOS_SCI_2019213215700_cor.fits</TD>\n'
         b'</TR>\n'
         b'<TR>\n'
         b'<TD>ad:NEOSS/NEOS_SCI_2019213215700.fits</TD>\n'
         b'</TR>\n'
         b'</TABLEDATA>\n'
         b'</DATA>\n'
         b'</TABLE>\n'
         b'<INFO name="QUERY_STATUS" value="OK" />\n'
         b'</RESOURCE>\n'
         b'</VOTABLE>\n']

    test_scrape._make_test_dirs()
    ftp_mock.return_value.__enter__.return_value.listdir. \
        side_effect = test_scrape._list_dirs
    ftp_mock.return_value.__enter__.return_value.stat. \
        side_effect = test_scrape._entry_stats

    tap_mock.return_value.__enter__.return_value = response
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        test_subject = validator.NeossatValidator()
        test_listing_fqn = \
            f'{test_subject._config.working_directory}/{mc.VALIDATE_OUTPUT}'
        test_source_list_fqn = f'{test_subject._config.working_directory}/' \
                               f'{scrape.NEOSSAT_SOURCE_LIST}'
        if os.path.exists(test_listing_fqn):
            os.unlink(test_listing_fqn)
        if os.path.exists(test_subject._config.work_fqn):
            os.unlink(test_subject._config.work_fqn)
        if os.path.exists(test_source_list_fqn):
            os.unlink(test_source_list_fqn)

        test_source, test_destination = test_subject.validate()
        assert test_source is not None, 'expected source result'
        assert test_destination is not None, 'expected destination result'
        assert len(test_source) == 1, 'wrong number of source results'
        assert 'NEOS_SCI_2019213215700_clean.fits' in test_source, \
            'wrong source content'
        assert len(test_destination) == 1, 'wrong # of destination results'
        assert 'NEOS_SCI_2019213215700_cor.fits' in test_destination, \
            'wrong destination content'
        assert os.path.exists(test_listing_fqn), 'should create file record'

        test_subject.write_todo()
        assert os.path.exists(test_subject._config.work_fqn), \
            'should create file record'
        with open(test_subject._config.work_fqn, 'r') as f:
            content = f.readlines()
        assert content == ['/users/OpenData_DonneesOuvertes/pub/NEOSSAT/'
                           'ASTRO/NEOS_SCI_2019213215700_clean.fits\n'], \
            'unexpected content'

        # does the cached list work too?
        test_cache = test_subject.read_list_from_source()
        assert test_cache is not None, 'expected cached source result'
        assert next(iter(test_cache)) == \
            'NEOS_SCI_2019213215700_cord.fits\n', \
            'wrong cached result'
    finally:
        os.getcwd = getcwd_orig
