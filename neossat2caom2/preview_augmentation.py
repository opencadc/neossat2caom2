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

import logging
import os
import re

from astropy.io import fits
import matplotlib.pyplot as plt
import matplotlib.colors as colors

from caom2 import Observation, ReleaseType, ProductType
from caom2pipe import manage_composable as mc
from neossat2caom2 import ARCHIVE, NEOSSatName


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)

    working_dir = kwargs.get('working_directory', './')
    cadc_client = kwargs.get('cadc_client')
    if cadc_client is None:
        logging.warning(
            'Visitor needs a cadc_client parameter to store images.')
    stream = kwargs.get('stream')
    if stream is None:
        raise mc.CadcException('Visitor needs a stream parameter.')
    observable = kwargs.get('observable')
    if observable is None:
        raise mc.CadcException('Visitor needs a observable parameter.')
    science_file = kwargs.get('science_file')

    count = 0
    for plane in observation.planes.values():
        delete_list = []
        for artifact in plane.artifacts.values():
            if artifact.uri.endswith(science_file):
                count += _do_prev(artifact, plane, working_dir, cadc_client,
                                  stream, observable)
            if artifact.uri.endswith('.jpg'):
                delete_list.append(artifact.uri)

        for uri in delete_list:
            plane.artifacts.pop(uri)

    logging.info('Completed preview augmentation for {}.'.format(
        observation.observation_id))
    return {'artifacts': count}


def _augment(plane, uri, fqn, product_type):
    temp = None
    if uri in plane.artifacts:
        temp = plane.artifacts[uri]
    plane.artifacts[uri] = mc.get_artifact_metadata(fqn, product_type,
                                                    ReleaseType.META, uri,
                                                    temp)


def _do_prev(artifact, plane, working_dir, cadc_client, stream, observable):
    naming = mc.CaomName(artifact.uri)
    neoss_name = NEOSSatName(file_name=naming.file_name)
    preview = neoss_name.prev
    preview_fqn = os.path.join(working_dir, preview)
    thumb = neoss_name.thumb
    thumb_fqn = os.path.join(working_dir, thumb)
    science_fqn = os.path.join(working_dir, naming.file_name)

    image_data = fits.getdata(science_fqn, ext=0)
    image_header = fits.getheader(science_fqn, ext=0)

    _generate_plot(preview_fqn, 1024, image_data, image_header)
    _generate_plot(thumb_fqn, 256, image_data, image_header)

    prev_uri = neoss_name.prev_uri
    thumb_uri = neoss_name.thumb_uri
    _store_smalls(cadc_client, working_dir, preview, thumb,
                  observable.metrics, stream)
    _augment(plane, prev_uri, preview_fqn, ProductType.PREVIEW)
    _augment(plane, thumb_uri, thumb_fqn, ProductType.THUMBNAIL)
    return 2


def _store_smalls(cadc_client, working_directory, preview_fname,
                  thumb_fname, metrics, stream):
    if cadc_client is not None:
        mc.data_put(cadc_client, working_directory, preview_fname, ARCHIVE,
                    stream, mime_type='image/png', metrics=metrics)
        mc.data_put(cadc_client, working_directory, thumb_fname, ARCHIVE,
                    stream, mime_type='image/png', metrics=metrics)


def _generate_plot(fqn, dpi_factor, image_data, image_header):
    # DB 07-10-19
    fig = plt.figure()
    dpi = fig.get_dpi()
    fig.set_size_inches(dpi_factor/dpi, dpi_factor/dpi)
    plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)

    # DB 08-10-19
    naxis1 = image_header.get('NAXIS1')
    naxis2 = image_header.get('NAXIS2')
    datasec = image_header.get('DATASEC')
    if datasec is None:
        xstart = ystart = 0
        xend = naxis1
        yend = naxis2
    else:
        dsl = list(map(
            int, re.split('\\[(-*\\d+):(-*\\d+),(-*\\d+):(-*\\d+)\\]',
                          datasec)[1:5]))
        if (naxis1 < dsl[0] or dsl[1] > naxis1 or
                naxis2 < dsl[2] or dsl[3] > naxis2 or
                dsl[1] - dsl[0] > naxis1 or dsl[3] - dsl[2] > naxis2):
            xstart = ystart = 0
            xend = naxis1
            yend = naxis2
        else:
            xstart = dsl[0] - 1
            xend = dsl[1]
            ystart = dsl[2] - 1
            yend = dsl[3]

    plt.imshow(image_data[ystart:yend, xstart:xend],
               cmap='gray_r',
               norm=colors.LogNorm())
    plt.axis('off')
    if os.access(fqn, 0):
        os.remove(fqn)
    plt.savefig(fqn, format='png')
    plt.close()
