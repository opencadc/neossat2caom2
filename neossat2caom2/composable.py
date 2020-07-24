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

"""
Implements the default entry point functions for the workflow application.

'run' executes based on either provided lists of work, or files on disk.
'run_state' executes incrementally, usually based on time-boxed intervals.
"""

import logging
import sys
import traceback

from datetime import datetime

from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from caom2pipe import run_composable as rc
from neossat2caom2 import APPLICATION, preview_augmentation
from neossat2caom2 import scrape, data_source, NEOSSatName

NEOS_BOOKMARK = 'neossat_timestamp'

META_VISITORS = []
DATA_VISITORS = [preview_augmentation]


def _run():
    """
    Uses a todo file to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    builder = nbc.FileNameBuilder(NEOSSatName)
    return rc.run_by_todo(name_builder=builder, command_name=APPLICATION,
                          meta_visitors=META_VISITORS,
                          data_visitors=DATA_VISITORS)


def run():
    """Wraps _run in exception handling, with sys.exit calls."""
    try:
        result = _run()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_state():
    """Uses a state file with a timestamp to control which files will be
    retrieved from the CSA ftp host.

    Ingestion is based on fully-qualified file names from the CSA ftp host,
    because those are difficult to reproduce otherwise.
    """
    builder = nbc.FileNameBuilder(NEOSSatName)
    config = mc.Config()
    config.get_executors()
    state = mc.State(config.state_fqn)
    start_time = state.get_bookmark(NEOS_BOOKMARK)
    temp = mc.increment_time(start_time, 0).timestamp()
    todo_list, max_timestamp = scrape.build_todo(
        temp, config.working_directory, config.state_fqn)
    max_date = datetime.fromtimestamp(max_timestamp)
    incremental_source = data_source.IncrementalSource(todo_list)
    return rc.run_by_state(config=config, name_builder=builder,
                           command_name=APPLICATION,
                           bookmark_name=NEOS_BOOKMARK,
                           meta_visitors=META_VISITORS,
                           data_visitors=DATA_VISITORS,
                           end_time=max_date, chooser=None,
                           source=incremental_source)


def run_state():
    """Wraps _run_state in exception handling."""
    try:
        _run_state()
        sys.exit(0)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)
