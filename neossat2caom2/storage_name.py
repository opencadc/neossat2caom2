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

from caom2pipe.manage_composable import StorageName


class NEOSSatName(StorageName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage
    """

    def __init__(self, file_name, source_names):
        super().__init__(file_name=file_name, source_names=source_names)
        self._logger.debug(self)

    def __str__(self):
        return f'\n' \
               f'      obs id: {self._obs_id}\n' \
               f'   file name: {self._file_name}\n' \
               f'source names: {self.source_names}\n'

    def is_valid(self):
        return True
    @property
    def prev(self):
        """The preview file name for the file."""
        return '{}_{}_prev.png'.format(self.obs_id, self._product_id)

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return '{}_{}_prev_256.png'.format(self.obs_id, self._product_id)

    @staticmethod
    def remove_extensions(value):
        return (
            value.replace('.fits', '')
            .replace('.header', '')
            .replace('.gz', '')
        )

    def set_file_id(self):
        self._file_id = NEOSSatName.remove_extensions(self._file_name)

    def set_obs_id(self):
        self._obs_id = NEOSSatName.extract_obs_id(self._file_id)

    def set_product_id(self):
        self._product_id = NEOSSatName.extract_product_id(self._file_id)

    @staticmethod
    def extract_obs_id(value):
        return (
            value.replace('_clean', '')
            .replace('NEOS_SCI_', '')
            .replace('_cord', '')
            .replace('_cor', '')
        )

    @staticmethod
    def extract_product_id(value):
        # DB 18-09-19
        # I think JJ suggested that product ID should be ‘cor’,  ‘cord’,
        # and so maybe ‘clean’.  i.e. depends on the trailing characters
        # after final underscore in the file name.  Perhaps ‘raw’ for files
        # without any such characters.
        result = 'raw'
        if '_cord' in value:
            result = 'cord'
        elif '_cor' in value:
            result = 'cor'
        elif '_clean' in value:
            result = 'clean'
        return result

    @staticmethod
    def is_preview(entry):
        return '.png' in entry
