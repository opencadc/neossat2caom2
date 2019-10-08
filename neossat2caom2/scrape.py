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

import logging
import stat
import traceback

from ftputil import FTPHost

from caom2pipe import manage_composable as mc

__all__ = ['build_todo']

ASC_FTP_SITE = 'ftp.asc-csa.gc.ca'
NEOS_DIR = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO'


def _build_todo(start_date, ftp_site, ftp_dir):
    """Recursively visit directories at the ftp site, looking for .fits
    files. If the file modification time is >= start_date, that file
    is a candidate for transfer.

    :return a dict, where keys are the fully-qualified file names on the
        ftp host server, and the values timestamps"""
    listing = {}
    try:
        with FTPHost(ftp_site, 'anonymous', '@anonymous') as ftp_host:
            dirs = ftp_host.listdir(ftp_dir)
            for entry in dirs:
                entry_fqn = '{}/{}'.format(ftp_dir, entry)
                entry_stats = ftp_host.stat(entry_fqn)
                if entry_stats.st_mtime >= start_date:
                    if stat.S_ISDIR(entry_stats.st_mode):
                        # True - it's a directory, follow it down later
                        listing[entry_fqn] = [True, entry_stats.st_mtime]
                    elif entry.endswith('.fits'):
                        # False - it's a file, just leave it in the list
                        listing[entry_fqn] = [False, entry_stats.st_mtime]
                        logging.info('Adding entry {}'.format(entry_fqn))
            ftp_host.close()

        temp_listing = {}
        for entry, value in listing.items():
            if value[0]:  # is a directory
                logging.info('Adding results for {}'.format(entry))
                temp_listing.update(_build_todo(start_date, ftp_site, entry))

        listing.update(temp_listing)

    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise mc.CadcException('Could not list {} on {}'.format(
            ftp_dir, ftp_site))
    return listing


def _remove_dir_names(item_list, start_date):
    """The listing that comes back from _build_todo contains directory
    names. This function removes the directory names from the list,
    using the boolean value set in the listing by the _build_todo
    function."""
    max_date = start_date
    todo_list = {}
    for entry, value in item_list.items():
        if not value[0]:
            todo_list[entry] = value[1]
            max_date = max(max_date, value[1])
    return todo_list, max_date


def build_todo(start_date):
    """
    Build a list of file names where the modification time for the file
    is >= start_time.

    :param start_date timestamp in seconds since the epoch
    :return a dict, where keys are file names on the ftp host server, and
        values are timestamps, plus the max timestamp from the ftp host
        server for file addition
    """
    logging.debug('Begin build_todo with date {}'.format(start_date))
    temp = _build_todo(start_date, ASC_FTP_SITE, NEOS_DIR)
    logging.error('temp is {}'.format(temp))
    todo_list, max_date = _remove_dir_names(temp, start_date)
    logging.info('End build_todo with {} records, date {}.'.format(
        len(todo_list), max_date))
    return todo_list, max_date
