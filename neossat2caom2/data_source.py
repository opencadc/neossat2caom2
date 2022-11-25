# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

from bs4 import BeautifulSoup
from collections import defaultdict, deque
from datetime import datetime, timezone

from caom2pipe import data_source_composable as dsc
from caom2pipe.manage_composable import get_endpoint_session, make_time_tz, query_endpoint_session


__all__ = ['CSADataSource']


class CSADataSource(dsc.DataSource):
    """
    Time-box the scraping of an https site organized by top-level page, all-the-days-in-a-year page, and
    all-the-files-from-a-day page.
    """

    def __init__(
        self, config, start_time=datetime(year=2018, month=10, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    ):
        """
        The default time is prior to the timestamp on the earliest file at the data source.
        The earliest dated file I could find on the original FTP site was 10-18-18

        :param config: Config
        :param start_time: datetime
        """
        super().__init__(config)
        self._data_sources = config.data_sources
        self._session = get_endpoint_session()
        self._start_time = start_time
        self._max_time = start_time.timestamp()
        # self._todo_list - the list of all work queried from the http data source, self._work is the time-boxed list
        # constructed using that content
        self._todo_list = defaultdict(list)

    @property
    def max_time(self):
        """:return datetime.timestamp"""
        return self._max_time

    @property
    def todo_list(self):
        """Just here for the mocking."""
        return self._todo_list

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Time-boxing the file url list returned from the site scrape.

        :param prev_exec_time timestamp start of the timestamp chunk
        :param exec_time timestamp end of the timestamp chunk
        :return: a list of StateRunnerMeta instances, for file names with time they were modified
        """
        self._logger.debug('Begin get_time_box_work')
        self.initialize_todo()
        self._work = deque()
        for time_stamp in sorted(self.todo_list):
            if prev_exec_time < time_stamp <= exec_time:
                for entry in self.todo_list[time_stamp]:
                    self._work.append(dsc.StateRunnerMeta(entry, time_stamp))
        self._capture_todo()
        self._logger.debug('End get_time_box_work')
        return self._work

    def get_work(self):
        """
        Consolidate the repeated query result of the CSA page into a list of URLs.
        :return: a list of URLs
        """
        self._logger.debug('Begin get_work')
        self.initialize_todo()
        self._work = deque()
        for entry in self._todo_list:
            self._work += self._todo_list[entry]
        self._capture_todo()
        self._logger.debug('End get_work')
        return self._work

    def initialize_todo(self):
        """Capture the list of work, based on timestamps from the CSA NEOSSat page in self._todo_list."""
        if len(self._todo_list) > 0:
            self._logger.info('Already initialized.')
            return

        for data_source in self._config.data_sources:
            response = None
            try:
                response = query_endpoint_session(data_source, self._session)
                if response is None:
                    self._logger.warning(f'Could not query {data_source}')
                else:
                    years = self._parse_top_page(data_source, response.text)
                    self._logger.info(f'Found {len(years)} years on {data_source}.')
                    response.close()

                    for year_list in years.values():
                        for year_url in year_list:
                            self._logger.debug(f'Checking year {year_url} on date {self._start_time}')
                            response = query_endpoint_session(year_url, self._session)
                            if response is None:
                                self._logger.warning(f'Could not query year {year_url}')
                            else:
                                days = self._parse_year_page(year_url, response.text)
                                response.close()
                                self._logger.info(f'Found {len(days)} days on {year_url}.')

                                # get the list of files
                                for day_list in days.values():
                                    for day_url in day_list:
                                        self._logger.debug(f'Checking day {day_url} with date {self._start_time}')
                                        response = query_endpoint_session(day_url, self._session)
                                        if response is None:
                                            self._logger.warning(f'Could not query {day_url}')
                                        else:
                                            files = self._parse_day_page(day_url, response.text)
                                            self._logger.info(f'Found {len(files)} files on {day_url}.')
                                            response.close()
                                            self._consolidate_lists_of_files_by_timestamp(files)
            finally:
                if response is not None:
                    response.close()

    def _consolidate_lists_of_files_by_timestamp(self, files):
        for dt in files:
            self._todo_list[dt.timestamp()] += files[dt]
            self._max_time = max(self._max_time, dt.timestamp())

    def _parse_day_page(self, root_url, html_string):
        """
        :return: dict, keys are timestamps, values are URLs, so the dict can be sorted
        """
        result = defaultdict(list)
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a')
        for href in hrefs:
            f_name = href.get('href')
            if f_name.endswith('.fits') or f_name.endswith('.fits.gz'):
                dt_str = href.next_element.next_element.string.replace('-', '').strip()
                dt = make_time_tz(dt_str)
                if dt >= self._start_time:
                    temp = f'{root_url}/{f_name}'
                    self._logger.debug(f'Adding file: {temp}')
                    result[dt].append(temp)
        return result

    def _parse_top_page(self, root_url, html_string):
        """
        :return: dict, keys are timestamps, values are URLs, so the dict can be sorted
        """
        result = defaultdict(list)
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a')
        for href in hrefs:
            y = href.get('href').replace('/', '')
            try:
                int_y = int(y)
            except ValueError as e:
                continue
            if y == 'NESS' or int(y) >= 2017:
                dt_str = href.next_element.next_element.string.replace('-', '').strip()
                dt = make_time_tz(dt_str)
                if dt >= self._start_time:
                    temp = f'{root_url}{y}'
                    self._logger.debug(f'Adding Top Level: {temp}')
                    result[dt].append(temp)
        return result

    def _parse_year_page(self, root_url, html_string):
        """
        :return: dict, keys are timestamps, values are URLs, so the dict can be sorted
        """
        result = defaultdict(list)
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a')
        for href in hrefs:
            d = href.get('href').replace('/', '')
            try:
                int_d = int(d)
            except ValueError as e:
                continue
            if 1 <= int_d <= 366:
                dt_str = href.next_element.next_element.string.replace('-', '').strip()
                dt = make_time_tz(dt_str)
                if dt >= self._start_time:
                    temp = f'{root_url}/{d}'
                    self._logger.debug(f'Adding Day: {temp}')
                    result[dt].append(temp)
        return result
