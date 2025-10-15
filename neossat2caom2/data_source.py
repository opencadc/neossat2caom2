# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2025.                            (c) 2025.
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

from os.path import basename

from caom2utils.data_util import get_local_file_info, get_local_file_headers
from caom2pipe.execute_composable import NoFheadStoreVisitRunnerMeta, OrganizeExecutesRunnerMeta
from caom2pipe.html_data_source import HtmlFilter, HtmlFilteredPagesTemplate
from caom2pipe.manage_composable import TaskType


__all__ = ['NeossatPagesTemplate']


def filter_by_year(href):
    """
    :param href str representation of an 'href' element from bs4
    """
    # There's no timestamp check for the top-level directories when harvesting incrementally because the
    # lower pages have products retroactively added that don't change the timestamps on the YEAR page.
    y = href.replace('/', '')
    try:
        return y == 'NESS' or int(y) >= 2017
    except ValueError:
        return False


class NeossatPagesTemplate(HtmlFilteredPagesTemplate):

    def __init__(self, config):
        super().__init__(config)
        # True - ignore datetime on the top page
        self._year_filter = HtmlFilter(filter_by_year, True)

    def add_children(self, to_node, in_tree, new_entries):
        # which template_filter gets added
        if in_tree.parent(to_node.identifier).is_root():
            template_filter = self._always_true_filter
        elif in_tree.parent(to_node.identifier).is_leaf():
            template_filter = self._always_true_filter
        else:
            template_filter = self._file_filter

        for url in new_entries:
            in_tree.create_node(url, parent=to_node.identifier, data=template_filter)

    def is_leaf(self, url_tree, url_node):
        return url_tree.depth(url_node) == 3

    def first_filter(self):
        return self._year_filter


class NEOSSATNoFheadStoreVisitRunnerMeta(NoFheadStoreVisitRunnerMeta):
    """
    This is a temporary class to support refactoring, and when all dependent applications have also been refactored
    to provide the expected StorageName API, this class will be integrated back into the CaomExecute class.
    """

    def _set_preconditions(self):
        """The default preconditions are ensuring that the StorageName instance from the 'context' parameter has
        both the metadata and file_info members initialized correctly. For the default case assume the files are
        found on local posix disk, and the preconditions are satisfied by local file access functions to the
        source_names values in StorageName."""
        if self._config.use_local_files:
            super()._set_preconditions()
        else:
            self._logger.debug('Do _set_preconditions after the store, at the interim storage location.')

    def _store_data(self):
        super()._store_data()
        # use the data staged locally to get the file info and header content, which is the default _set_preconditions
        # implementation
        self._logger.debug(f'Begin _set_preconditions for {self._storage_name.file_name}')
        for index, source_name in enumerate(self._storage_name.source_names):
            uri = self._storage_name.destination_uris[index]
            interim_name = f'{self._config.working_directory}/{self._storage_name.obs_id}/{basename(source_name)}'
            self._logger.error(interim_name)
            if uri not in self._storage_name.file_info:
                self._storage_name.file_info[uri] = get_local_file_info(interim_name)
            if uri not in self._storage_name.metadata:
                self._storage_name.metadata[uri] = []
            if '.fits' in source_name:
                self._storage_name._metadata[uri] = get_local_file_headers(interim_name)
        self._logger.debug('End _set_preconditions')


class NEOSSatOrganizeExecutesRunnerMeta(OrganizeExecutesRunnerMeta):
    """A class that extends OrganizeExecutes to handle the choosing of the correct executors based on the config.yml.
    Attributes:
        _needs_delete (bool): if True, the CAOM repo action is delete/create instead of update.
        _reporter: An instance responsible for reporting the execution status.
    Methods:
        _choose():
            Determines which descendants of CaomExecute to instantiate based on the content of the config.yml
            file for an application.

    This is a temporary class to support refactoring, and when all dependent applications have also been refactored
    to provide the expected StorageName API, this class will be integrated back into the CaomExecute class.

    """

    def _choose(self):
        """The logic that decides which descendants of CaomExecute to instantiate. This is based on the content of
        the config.yml file for an application.
        """
        super()._choose()
        if self.can_use_single_visit() and TaskType.STORE in self.task_types and not self.config.use_local_files:
            self._logger.debug(f'Choosing executor NEOSSatOrganizeExecutesRunnerMeta to over-ride default choice.')
            self._executors = []  # over-ride the default choice.
            self._executors.append(
                NEOSSATNoFheadStoreVisitRunnerMeta(
                    self.config,
                    self._clients,
                    self._store_transfer,
                    self._meta_visitors,
                    self._data_visitors,
                    self._reporter,
                )
            )
