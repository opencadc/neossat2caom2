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

import importlib
import logging
import math
import os
import sys
import traceback

from caom2 import Observation, CalibrationLevel, Axis, CoordAxis1D
from caom2 import RefCoord, SpectralWCS, CoordRange1D, Chunk
from caom2 import CoordFunction2D, Dimension2D, Coord2D, ProductType
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2utils import WcsParser
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from caom2pipe import execute_composable as ec


__all__ = ['neossat_main_app', 'update', 'NEOSSatName', 'COLLECTION',
           'APPLICATION', 'ARCHIVE']


APPLICATION = 'neossat2caom2'
COLLECTION = 'NEOSSAT'
ARCHIVE = 'NEOSS'


class NEOSSatName(ec.StorageName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage
    """

    BLANK_NAME_PATTERN = '*'

    def __init__(self, obs_id=None, fname_on_disk=None, file_name=None):
        if obs_id is not None:
            fname_on_disk = 'NEOS_SCI_{}.fits'.format(obs_id)
        elif file_name is not None:
            if '/' in file_name:
                self._ftp_fqn = file_name
                file_name = os.path.basename(file_name)
            fname_on_disk = file_name
            obs_id = NEOSSatName.remove_extensions(
                NEOSSatName.extract_obs_id(file_name))
            self._product_id = NEOSSatName.extract_product_id(file_name)
        self._file_name = fname_on_disk.replace('.header', '')
        super(NEOSSatName, self).__init__(
            obs_id, COLLECTION, NEOSSatName.BLANK_NAME_PATTERN, fname_on_disk,
            archive=ARCHIVE)
        logging.debug('obs id {} file name {}'.format(
            self._obs_id, self._file_name))

    def is_valid(self):
        return True

    @property
    def product_id(self):
        return self._product_id

    @property
    def file_name(self):
        """The file name."""
        return self._file_name

    @property
    def file_uri(self):
        """The ad URI for the file. Assumes compression."""
        return '{}:{}/{}'.format(self.scheme, self.archive, self.file_name)

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
        return value.replace('.fits', '').replace('.header', '')

    @staticmethod
    def extract_obs_id(value):
        return value.replace('_clean', '').replace('NEOS_SCI_', '').replace(
            '_cord', '').replace('_cor', '')

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


def get_artifact_product_type(header):
    result = ProductType.SCIENCE
    obs_intent = get_obs_intent(header)
    if obs_intent == 'calibration':
        result = ProductType.CALIBRATION
    return result


def get_calibration_level(uri):
    cal_level = CalibrationLevel.RAW_STANDARD
    if 'clean' in uri or 'cor' in uri:
        cal_level = CalibrationLevel.CALIBRATED
    return cal_level


def get_coord1_pix(header):
    ccdsec = header.get('CCDSEC')
    pix = ccdsec.split(',')[0].split(':')[1]
    return pix


def get_coord2_pix(header):
    ccdsec = header.get('CCDSEC')
    pix = ccdsec.split(',')[1].split(':')[1]
    return pix


def get_dec(header):
    ra_deg_ignore, dec_deg = _get_position(header)
    return dec_deg


def get_obs_intent(header):
    # DB 17-10-19
    # For Observation.intent, ignore the INTENT keyword now.  Instead,
    # if MODE value contains “FINE_POINT” or “FINE_SLEW”:
    #   Observation.intent = ‘science’.
    # else:
    #   Observation.intent = ‘calibration’.
    # Don’t do an exact match since the values have numerical components,
    # e.g. “16 - FINE_POINT”.   I haven’t come across a file that does NOT have
    # the MODE keyword (unlike INTENT) - but not sure what to set the ‘intent’
    # to if there isn’t one.  I guess ‘science’ like caom2utils does?

    # Just in case there are observations with OBJECT = DARK but also with
    # FINE_POINT or FINE_SLEW modes, in get_obs_type set Observation.intent
    # to ‘calibration’ if this function finds “result =  ‘dark’ “.   i.e. add
    # code after the “result = result.lower()” line to set
    # Observation.intent = ‘calibration’.

    # According to Dave Balam these are all calibration:   FINE_SETTLE,
    # ST_REACQUIRE and RATE_BRAKE, ST_ACQUIRE, EKF_SETTLE, 21 - DESAT,
    # COARSE_SETTLE, COARSE_BRAKE, COURSE_SLEW
    # Dave Balam also said this one appears on occasion:  XX - N/A” TBD

    # Please add “FINE_HOLD” as a 3rd string for a ‘science’ observation.

    result = 'calibration'
    mode = header.get('MODE')
    if (mode is not None and
            ('FINE_POINT' in mode or 'FINE_SLEW' in mode or
             'FINE_HOLD' in mode)):
        obs_type = get_obs_type(header)
        if obs_type != 'dark':
            result = 'science'
    return result


def get_obs_type(header):
    # DB 18-10-19
    # And Dave thinks that if MODE contains the string “DESAT” then that
    # observation is going to have an observation.type of ‘dark’.  So maybe
    # change get_obs_type() to:

    # def get_obs_type(header):
    #    result = header.get(‘OBJECT’)
    #    mode = header.get(‘MODE’)
    #    if “DESAT” in mode:
    #        result = ‘DARK’
    #    if result is not None and result == ‘DARK’:
    #        result = result.lower()
    #    else:
    #        result = ‘object’
    #    return result
    # (I wonder if ‘dark’ is ever the OBJECT value instead of ‘DARK’?
    # AS always put the object search string in lower case.  I’ll have to try
    # an ADQL query…)

    mode = header.get('MODE')
    if 'DESAT' in mode:
        result = 'dark'
    else:
        obj = header.get('OBJECT')
        if obj is not None and (obj == 'DARK' or obj == 'dark'):
            result = obj.lower()
        else:
            result = 'object'
    return result


def get_plane_data_release(header):
    result = header.get('RELEASE')
    if result is None:
        result = header.get('DATE-OBS')
    return result


def get_position_axis_function_naxis1(header):
    result = mc.to_int(header.get('NAXIS1'))
    if result is not None:
        result = result / 2.0
    return result


def get_position_axis_function_naxis2(header):
    result = mc.to_int(header.get('NAXIS2'))
    if result is not None:
        result = result / 2.0
    return result


def get_ra(header):
    ra_deg, dec_deg_ignore = _get_position(header)
    return ra_deg


def get_target_moving(header):
    result = True
    moving = header.get('MOVING')
    if moving == 'F':
        result = False
    return result


def get_target_type(header):
    result = header.get('TARGTYPE')
    if result is not None:
        result = result.lower()
    return result


def get_time_delta(header):
    exptime = mc.to_float(header.get('EXPOSURE'))  # in s
    return exptime / (24.0 * 3600.0)


def get_time_function_val(header):
    time_string = header.get('DATE-OBS')
    return ac.get_datetime(time_string)


def _get_energy(header):
    # DB 24-09-19
    # if bandpass IS None: set min_wl to 0.4, max_wl to 0.9 (microns)
    min_wl = 0.4
    max_wl = 0.9
    # header units are Angstroms
    bandpass = header.get('BANDPASS')
    if bandpass is not None:
        temp = bandpass.split(',')
        min_wl = mc.to_float(temp[0]) / 1e4
        max_wl = mc.to_float(temp[1]) / 1e4
    return min_wl, max_wl


def _get_position(header):
    ra = header.get('RA')
    dec = header.get('DEC')
    if ra is None and dec is None:
        # DB 25-09-19
        # Looking at other sample headers for a bunch of files OBJCTRA and
        # OBJCTDEC are always the same as RA and DEC so use those if RA and/or
        # DEC are missing  Note OBJCRA/OBJCTDEC do not have ‘:’ delimiters
        # between hours(degrees)/minutes/seconds.
        ra_temp = header.get('OBJCTRA')
        dec_temp = header.get('OBJCTDEC')
        ra = ra_temp.replace(' ', ':')
        dec = dec_temp.replace(' ', ':')
    if ra is not None and dec is not None:
        ra, dec = ac.build_ra_dec_as_deg(ra, dec)
    return ra, dec


def accumulate_bp(bp, uri):
    """Configure the telescope-specific ObsBlueprint at the CAOM model 
    Observation level.

    Guidance for construction is available from this doc:
    https://docs.google.com/document/d/1Z84x9t2iCK72j3-u6LSejYiDi58097oP4PY1up0axjI/edit#
    """

    logging.debug('Begin accumulate_bp.')

    bp.set('Observation.intent', 'get_obs_intent(header)')
    # DB 24-09-19
    # If OBSTYPE not in header, set target.type = ‘object’
    # If observation is a 'DARK', set target.type = 'dark'
    bp.set('Observation.type', 'get_obs_type(header)')

    bp.clear('Observation.instrument.name')
    bp.add_fits_attribute('Observation.instrument.name', 'INSTRUME')
    # DB 24-09-19
    # If INSTRUME not in header, set Observation.instrument.name =
    # ‘NEOSSat_Science’
    bp.set_default('Observation.instrument.name', 'NEOSSat_Science')
    # DB 17-10-19
    # Set Observation.instrument.keywords to the value of the MODE keyword.
    bp.clear('Observation.instrument.keywords')
    bp.add_fits_attribute('Observation.instrument.keywords', 'MODE')

    bp.set('Observation.target.type', 'get_target_type(header)')
    bp.set('Observation.target.moving', 'get_target_moving(header)')

    bp.clear('Observation.proposal.id')
    bp.add_fits_attribute('Observation.proposal.id', 'PROP_ID')

    bp.clear('Plane.metaRelease')
    bp.add_fits_attribute('Plane.metaRelease', 'DATE-OBS')
    bp.set('Plane.dataRelease', 'get_plane_data_release(header)')

    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 'get_calibration_level(uri)')

    bp.clear('Plane.provenance.name')
    bp.add_fits_attribute('Plane.provenance.name', 'CONV_SW')
    bp.clear('Plane.provenance.version')
    bp.add_fits_attribute('Plane.provenance.version', 'CONV_VER')
    bp.clear('Plane.provenance.producer')
    bp.add_fits_attribute('Plane.provenance.producer', 'CREATOR')
    bp.clear('Plane.provenance.runID')
    bp.add_fits_attribute('Plane.provenance.runID', 'RUNID')

    bp.set_default('Artifact.releaseType', 'data')
    bp.set('Artifact.productType', 'get_artifact_product_type(header)')

    bp.configure_time_axis(3)
    bp.set('Chunk.time.axis.axis.ctype', 'TIME')
    bp.set('Chunk.time.axis.axis.cunit', 'd')
    bp.set('Chunk.time.axis.function.naxis', '1')
    bp.set('Chunk.time.axis.function.delta', 'get_time_delta(header)')
    bp.set('Chunk.time.axis.function.refCoord.pix', '0.5')
    bp.set('Chunk.time.axis.function.refCoord.val',
           'get_time_function_val(header)')
    bp.clear('Chunk.time.exposure')
    bp.add_fits_attribute('Chunk.time.exposure', 'EXPOSURE')

    bp.configure_polarization_axis(5)
    bp.configure_observable_axis(6)

    logging.debug('Done accumulate_bp.')


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')
    mc.check_param(observation, Observation)

    headers = None
    if 'headers' in kwargs:
        headers = kwargs['headers']
    fqn = None
    if 'fqn' in kwargs:
        fqn = kwargs['fqn']

    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            for part in artifact.parts.values():
                for chunk in part.chunks:
                    _build_chunk_energy(chunk, headers)
                    _build_chunk_position(
                        chunk, headers, observation.observation_id)

    logging.debug('Done update.')
    return observation


def _build_chunk_energy(chunk, headers):
    # DB 18-09-19
    # NEOSSat folks wanted the min/max wavelengths in the BANDPASS keyword to
    # be used as the upper/lower wavelengths.  BANDPASS = ‘4000,9000’ so
    # ref_coord1 = RefCoord(0.5, 4000) and ref_coord2 = RefCoord(1.5, 9000).
    # The WAVELENG value is not used for anything since they opted to do it
    # this way.  They interpret WAVELENG as being the wavelengh of peak
    # throughput of the system I think.

    min_wl, max_wl = _get_energy(headers[0])
    axis = CoordAxis1D(axis=Axis(ctype='WAVE', cunit='um'))
    if min_wl is not None and max_wl is not None:
        ref_coord1 = RefCoord(0.5, min_wl)
        ref_coord2 = RefCoord(1.5, max_wl)
        axis.range = CoordRange1D(ref_coord1, ref_coord2)

        # DB 24-09-19
        # If FILTER not in header, set filter_name = ‘CLEAR’
        filter_name = headers[0].get('FILTER', 'CLEAR')

        # DB 24-09-19
        # if wavelength IS None, wl = 0.6 microns, and resolving_power is
        # always determined.
        resolving_power = None
        wavelength = headers[0].get('WAVELENG', 6000)
        wl = wavelength / 1e4  # everything in microns
        resolving_power = wl / (max_wl - min_wl)
        energy = SpectralWCS(axis=axis,
                             specsys='TOPOCENT',
                             ssyssrc='TOPOCENT',
                             ssysobs='TOPOCENT',
                             bandpass_name=filter_name,
                             resolving_power=resolving_power)
        chunk.energy = energy
        chunk.energy_axis = 4


def _build_chunk_position(chunk, headers, obs_id):
    # DB 18-08-19
    # Ignoring rotation for now:  Use CRVAL1 = RA from header, CRVAL2 = DEC
    # from header.  NAXIS1/NAXIS2 values gives number of pixels along RA/DEC
    # axes (again, ignoring rotation) and assume CRPIX1 = NAXIS1/2.0 and
    # CRPIX2 = NAXIS2/2.0 (i.e. in centre of image).   XBINNING/YBINNING
    # give binning values along RA/DEC axes.   CDELT1 (scale in
    # degrees/pixel; it’s 3 arcsec/unbinned pixel)= 3.0*XBINNING/3600.0
    # CDELT2 = 3.0*YBINNING/3600.0.  Set CROTA2=0.0
    header = headers[0]
    header['CTYPE1'] = 'RA---TAN'
    header['CTYPE2'] = 'DEC--TAN'
    header['CUNIT1'] = 'deg'
    header['CUNIT2'] = 'deg'
    header['CRVAL1'] = get_ra(header)
    header['CRVAL2'] = get_dec(header)
    header['CRPIX1'] = get_position_axis_function_naxis1(header)
    header['CRPIX2'] = get_position_axis_function_naxis2(header)

    wcs_parser = WcsParser(header, obs_id, 0)
    if chunk is None:
        chunk = Chunk()
    wcs_parser.augment_position(chunk)

    x_binning = header.get('XBINNING')
    if x_binning is None:
        x_binning = 1.0
    cdelt1 = 3.0 * x_binning / 3600.0
    y_binning = header.get('YBINNING')
    if y_binning is None:
        y_binning = 1.0
    cdelt2 = 3.0 * y_binning / 3600.0

    objct_rol = header.get('OBJCTROL')
    if objct_rol is None:
        objct_rol = 0.0

    crota2 = 90.0 - objct_rol
    crota2_rad = math.radians(crota2)
    cd11 = cdelt1 * math.cos(crota2_rad)
    cd12 = abs(cdelt2) * _sign(cdelt1) * math.sin(crota2_rad)
    cd21 = -abs(cdelt1) * _sign(cdelt2) * math.sin(crota2_rad)
    cd22 = cdelt2 * math.cos(crota2_rad)

    dimension = Dimension2D(header.get('NAXIS1'),
                            header.get('NAXIS2'))
    x = RefCoord(get_position_axis_function_naxis1(header),
                 get_ra(header))
    y = RefCoord(get_position_axis_function_naxis2(header),
                 get_dec(header))
    ref_coord = Coord2D(x, y)
    function = CoordFunction2D(dimension,
                               ref_coord,
                               cd11,
                               cd12,
                               cd21,
                               cd22)
    chunk.position.axis.function = function
    chunk.position_axis_1 = 1
    chunk.position_axis_2 = 2


def _sign(value):
    return -1.0 if value < 0.0 else 1.0


def _build_blueprints(uris):
    """This application relies on the caom2utils fits2caom2 ObsBlueprint
    definition for mapping FITS file values to CAOM model element
    attributes. This method builds the DRAO-ST blueprint for a single
    artifact.

    The blueprint handles the mapping of values with cardinality of 1:1
    between the blueprint entries and the model attributes.

    :param uri The artifact URI for the file to be processed."""
    module = importlib.import_module(__name__)
    blueprints = {}
    for uri in uris:
        blueprint = ObsBlueprint(module=module)
        if not ec.StorageName.is_preview(uri):
            accumulate_bp(blueprint, uri)
        blueprints[uri] = blueprint
    return blueprints


def _get_uris(args):
    result = []
    if args.local:
        for ii in args.local:
            file_id = NEOSSatName.remove_extensions(os.path.basename(ii))
            file_name = '{}.fits'.format(file_id)
            result.append(NEOSSatName(file_name=file_name).file_uri)
    elif args.lineage:
        for ii in args.lineage:
            result.append(ii.split('/', 1)[1])
    else:
        raise mc.CadcException(
            'Could not define uri from these args {}'.format(args))
    return result


def neossat_main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        uris = _get_uris(args)
        blueprints = _build_blueprints(uris)
        gen_proc(args, blueprints)
    except Exception as e:
        logging.error('Failed {} execution for {}.'.format(APPLICATION, args))
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)

    logging.debug('Done {} processing.'.format(APPLICATION))
