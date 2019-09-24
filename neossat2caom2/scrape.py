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
import traceback

from ftplib import FTP
from caom2pipe import manage_composable as mc

__all__ = ['build_todo', 'set_start_time']

ASC_FTP_SITE = 'ftp.asc-csa.gc.ca'
NEOS_DIR = '/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/'


ftp = None
start_time = None
result = {}
sub_dir = None


def get_start_time():
    global start_time
    return start_time


def set_start_time(to_time):
    global start_time
    start_time = to_time


def _parse_retln(ftp_line):
    temp = ftp_line.split()
    dir_name = temp[-1].strip()
    dir_time = '{} {} {}'.format(temp[5], temp[6], temp[7])
    dir_timestamp = mc.make_seconds(dir_time)
    return dir_name, dir_timestamp


def _parse_line(ftp_line):
    global sub_dir
    global ftp
    global result
    if ftp_line.startswith('d') and not ftp_line.endswith('.'):
        dir_name, dir_timestamp = _parse_retln(ftp_line)
        if dir_timestamp >= get_start_time():
            sub_dir = '{}/{}'.format(sub_dir, dir_name)
            logging.info('Checking {} for new files.'.format(sub_dir))
            ftp.cwd(sub_dir)
            # ftp.retrlines('LIST', callback=_parse_line)
            # return
    elif ftp_line.startswith('-r') and ftp_line.endswith('.fits'):
        f_name, f_timestamp = _parse_retln(ftp_line)
        if f_timestamp >= get_start_time():
            f_dir = '{}/{}'.format(sub_dir, f_name)
            result[f_dir] = f_timestamp
            logging.debug('Found {} at time {}'.format(f_dir, f_timestamp))


def _build_todo(start_date, ftp_site, ftp_dir):
    """
    Build a list of URLs where the timestamp is >= start_time. This function
    exists to support testing.

    :param start_date timestamp in seconds since the epoch
    :return a dict, where keys are URLs, and values are timestamps
    """
    global start_time
    global ftp
    global sub_dir
    start_time = start_date
    max_date = get_start_time()

    logging.debug('Begin build_todo with date {}'.format(get_start_time()))
    try:
        ftp = FTP(ftp_site)
        ftp.login()  # anonymous
        sub_dir = ftp_dir
        ftp.cwd(sub_dir)
        ftp.retrlines('LIST', callback=_parse_line)
    except Exception as e:
        logging.error(
            'Failed ftp connection to {} with {}'.format(ASC_FTP_SITE, e))
        logging.debug(traceback.format_exc())
        raise e  # TODO
    finally:
        if ftp is not None:
            ftp.close()
    logging.debug('End build_todo with {} records, date {}'.format(
        len(result), max_date))
    return result, max_date


def build_todo(start_date):
    """
    Build a list of URLs where the timestamp is >= start_time

    :param start_date timestamp in seconds since the epoch
    :return a dict, where keys are URLs, and values are timestamps
    """
    logging.debug('Begin build_todo with date {}'.format(get_start_time()))
    todo_list, max_date = _build_todo(start_date, ASC_FTP_SITE, NEOS_DIR)
    logging.debug('End build_todo with {} records, date {}'.format(
        len(todo_list), max_date))
    return todo_list, max_date
