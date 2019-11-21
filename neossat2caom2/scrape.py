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
import os
import stat
import traceback

from ftputil import FTPHost, error

from caom2pipe import manage_composable as mc

__all__ = ['build_todo', 'list_for_validate']

ASC_FTP_SITE = 'ftp.asc-csa.gc.ca'
NEOS_DIR = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO'
# the earliest dated file I could find on the FTP site was 10-18-18
NEOSSAT_START_DATE = '2018-10-01T00:00:00.000'
NEOSSAT_CACHE = 'source_cache.csv'
NEOSSAT_SOURCE_LIST = 'source_listing.yml'
NEOSSAT_DIR_LIST = 'source_dir_listing.csv'
# information found in the state file
NEOS_CONTEXT = 'neossat_context'


def _append_source_listing(start_date, sidecar_dir, current):
    """Recursively visit directories at the ftp site, looking for .fits
    files. If the file modification time is >= start_date, that file
    is a candidate for transfer.

    Use two local files as a cache of listing information, for better
    repeatability. The first file is a list of the directories that have
    been successfully visited, so it's possible to not visit them again.
    The second file is the interim listing of the visit results, before
    the entries have been made unique, and the file/dir identification has
    been removed.

    :return a dict, where keys are the fully-qualified file names on the
        ftp host server, and the values timestamps"""

    logging.debug('Begin build_todo with date {}'.format(start_date))

    # get the cache of directory names that have been listed
    dir_list_fqn = os.path.join(sidecar_dir, NEOSSAT_DIR_LIST)
    original_dirs_list = {}
    if os.path.exists(dir_list_fqn):
        with open(dir_list_fqn, 'r') as f:
            for line in f:
                temp = line.split(',')
                original_dirs_list[temp[0]] = temp[1]

    # use that cache, plus the original content listing, to finish the
    # source site listing
    temp = _append_todo(start_date, sidecar_dir, ASC_FTP_SITE, NEOS_DIR,
                        current, original_dirs_list)
    todo_list, max_date = _remove_dir_names(temp, start_date)
    logging.info('End build_todo with {} records, date {}.'.format(
        len(todo_list), max_date))
    return todo_list, max_date


def _append_todo(start_date, sidecar_dir, ftp_site, ftp_dir, listing,
                 original_dirs_list):
    try:
        with FTPHost(ftp_site, 'anonymous', '@anonymous') as ftp_host:
            dirs = ftp_host.listdir(ftp_dir)
            for entry in dirs:
                entry_fqn = '{}/{}'.format(ftp_dir, entry)
                entry_stats = ftp_host.stat(entry_fqn)
                if entry_stats.st_mtime >= start_date:
                    if stat.S_ISDIR(entry_stats.st_mode):
                        # True - it's a directory, follow it down later
                        if entry_fqn not in original_dirs_list:
                            listing[entry_fqn] = [True, entry_stats.st_mtime]
                    elif entry.endswith('.fits'):
                        # False - it's a file, just leave it in the list
                        listing[entry_fqn] = [False, entry_stats.st_mtime]
                        logging.debug('Adding entry {}'.format(entry_fqn))
            ftp_host.close()

        temp_listing = {}
        for entry, value in listing.items():
            if value[0] and entry not in original_dirs_list:  # is a directory
                temp_listing.update(
                    _append_todo(start_date, sidecar_dir, ftp_site, entry,
                                 temp_listing, original_dirs_list))
                _sidecar(entry, value, sidecar_dir)
                original_dirs_list[entry] = value[1]
                logging.info('Added results for {}'.format(entry))

        _cache(temp_listing, sidecar_dir)
        listing.update(temp_listing)

    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise mc.CadcException('Could not list {} on {}'.format(
            ftp_dir, ftp_site))
    return listing


def _cache(content, in_dir):
    # appending to a file makes no checks for uniqueness
    fqn = os.path.join(in_dir, NEOSSAT_CACHE)
    with open(fqn, 'a') as f:
        for key, value in content.items():
            f.write(f'{key}, {value[0]}, {value[1]}\n')


def _read_cache(in_dir):
    content = {}
    fqn = os.path.join(in_dir, NEOSSAT_CACHE)
    if os.path.exists(fqn):
        with open(fqn, 'r') as f:
            for line in f:
                temp = line.split(',')
                content[temp[0]] = [bool(temp[1].strip()),
                                    mc.to_float(temp[2].strip())]
    return content


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


def _sidecar(entry, meta, sidecar_dir):
    fqn = os.path.join(sidecar_dir, NEOSSAT_DIR_LIST)
    with open(fqn, 'a') as f:
        f.write(f'{entry}, {meta[1]}\n')


def build_todo(start_date, sidecar_dir, state_fqn):
    """
    Build a list of file names where the modification time for the file
    is >= start_time.

    :param start_date timestamp in seconds since the epoch
    :param sidecar_dir where to cache ftp directory listing progress
    :param state_fqn where to find the configurable list of sub-directories,
        for bookmarked queries
    :return a dict, where keys are file names on the ftp host server, and
        values are timestamps, plus the max timestamp from the ftp host
        server for file addition
    """
    logging.debug('Begin build_todo with date {}'.format(start_date))
    temp = {}
    state = mc.State(state_fqn)
    sub_dirs = state.get_context(NEOS_CONTEXT)
    # query the sub-directories of the root directory, because the timestamps
    # do not bubble up for modifications, only for additions
    for subdir in sub_dirs:
        query_dir = os.path.join(NEOS_DIR, subdir)
        temp.update(
            _append_todo(start_date, sidecar_dir, ASC_FTP_SITE, query_dir, {},
                         {}))
    todo_list, max_date = _remove_dir_names(temp, start_date)
    logging.info('End build_todo with {} records, date {}.'.format(
        len(todo_list), max_date))
    return todo_list, max_date


def list_for_validate(config):
    """
    :return: A dict, where keys are file names available from the CSA Open Data
        ftp site, and values are the timestamps for the files at the CSA site.
        available from the CSA Open Data ftp site, and values are the
        fully-qualified names at the CSA site, suitable for providing 'pull'
        task type content for a todo file.
    """
    list_fqn = os.path.join(config.working_directory, NEOSSAT_SOURCE_LIST)
    if os.path.exists(list_fqn):
        logging.debug(f'Retrieve content from existing file {list_fqn}')
        temp = mc.read_as_yaml(list_fqn)
        # 0 - False indicates a file, True indicates a directory
        # 1 - timestamp
        current = {key: [False, value] for key, value in temp.items()}
    else:
        # current will be empty if there's no cache
        current = _read_cache(config.working_directory)

    ts_s = mc.make_seconds(NEOSSAT_START_DATE)
    temp, ignore_max_date = _append_source_listing(
        ts_s, config.working_directory, current)
    mc.write_as_yaml(temp, list_fqn)

    # remove the fully-qualified path names from the validator list
    # while creating a dictionary where the file name is the key, and the
    # fully-qualified file name at the FTP site is the value
    validator_list = {ii.split('/')[-1]: ii for ii in temp}
    result = {ii.split('/')[-1]: temp[ii] for ii in temp}
    return result, validator_list
