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
import traceback

from os.path import join

from caom2pipe.manage_composable import StorageName, write_as_yaml
from caom2pipe.validator_composable import Validator
from neossat2caom2.data_source import CSADataSource


__all__ = ['NeossatValidator', 'validate']

NEOSSAT_SOURCE_LIST = 'source_listing.yml'
# the earliest dated file I could find on the FTP site was 10-18-18
NEOSSAT_START_DATE = '2018-10-01T00:00:00.000'


class NeossatValidator(Validator):
    def __init__(self):
        super(NeossatValidator, self).__init__(
            source_name=StorageName.collection,
            preview_suffix='png',
            source_tz='America/Montreal',
        )
        # a dictionary where the file name is the key, and the fully-qualified
        # file name at the FTP site is the value
        self._fully_qualified_list = None

    def read_from_source(self):
        validator_list, fully_qualified_list = list_for_validate(self._config)
        self._fully_qualified_list = fully_qualified_list
        from pandas import DataFrame, Series
        return DataFrame(
            {
                'url': Series(validator_list['url'], dtype='object'),
                'f_name': Series(validator_list['f_name'], dtype='object'),
                'dt': Series(validator_list['dt'], dtype='datetime64[ns]'),
            },
        )

    def write_todo(self):
        if len(self._source_missing) == 0 and len(self._destination_older) == 0:
            logging.info(f'No entries to write to {self._config.work_fqn}')
        else:
            with open(self._config.work_fqn, 'w') as f:
                for entry in self._source_missing:
                    f.write(f'{self._fully_qualified_list[entry]}\n')
                for entry in self._destination_older:
                    f.write(f'{self._fully_qualified_list[entry]}\n')


def list_for_validate(config):
    """
    :return: A dict, where keys are file names available from the CSA Open Data
        https site, and values are the timestamps for the files at the CSA site.
    """
    # want the whole list for validation, so pick the earliest timestamp, which is also the default timestamp
    data_source = CSADataSource(config)
    temp = data_source.get_work()

    list_fqn = join(config.working_directory, NEOSSAT_SOURCE_LIST)
    write_as_yaml(temp, list_fqn)

    # remove the fully-qualified path names from the validator list
    # while creating a dictionary where the file name is the key, and the
    # fully-qualified file name at the HTTPS site is the value
    validator_list = {ii.split('/')[-1]: ii for ii in temp}
    result = {'url': [], 'f_name': [], 'dt': []}
    for dt, file_list in data_source._todo_list.items():
        for f in file_list:
            result['url'].append(f)
            result['f_name'].append(f.split('/')[-1])
            result['dt'].append(dt)
    return result, validator_list


def validate():
    validator = NeossatValidator()
    validator.validate()
    validator.write_todo()


if __name__ == '__main__':
    import sys

    try:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        validate()
        sys.exit(0)
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        sys.exit(-1)
