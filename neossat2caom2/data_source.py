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

from caom2pipe.html_data_source import HtmlFilter, HtmlFilteredPagesTemplate


__all__ = ['NeossatPagesTemplate']


def filter_by_year(href):
    """
    :param href str representation of an 'href' element from bs4
    """
    # There's no timestamp check for the top-level directories when harvesting incrementally because the
    # lower pages have products retroactively added that don't change the timestamps on the YEAR page.
    #
    # VA 23-03-23
    # We are finally starting to push some of our “advanced” image products to the CSA Open Data.  These
    # are using new improved “cleaning” software, and the outputs are:
    # ·         *_cor.fits.gz   (Cropped, Overscan-corrected)
    # ·         *_cord.fits.gz  (Cropped, Overscan-corrected and dark-corrected)
    # For now, there is a set of data from day 2022-255 to 2022-272 (Didymos & more), but we will keep
    # populating these slowly, including the back-archive.
    #
    # Could you start picking [these files] up, so that all CADC users could eventually benefit from the
    # better-quality products?  Eventually, these will replace the “_clean.fits” products, but for now,
    # the “_cor.fits” and “_clean.fits” will both exist for some images.
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
