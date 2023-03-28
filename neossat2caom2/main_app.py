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

import math

from caom2 import CalibrationLevel, Axis, CoordAxis1D
from caom2 import RefCoord, SpectralWCS, CoordRange1D, Chunk, TypedOrderedDict
from caom2 import CoordFunction2D, Dimension2D, Coord2D, ProductType, Part
from caom2utils.caom2blueprint import FitsWcsParser, update_artifact_meta
from caom2pipe import astro_composable as ac
from caom2pipe.caom_composable import TelescopeMapping
from caom2pipe import manage_composable as mc


__all__ = ['APPLICATION', 'NEOSSatMapping']


APPLICATION = 'neossat2caom2'


class NEOSSatMapping(TelescopeMapping):

    def __init__(self, storage_name, headers, clients):
        super().__init__(storage_name, headers, clients)

    def get_artifact_product_type(self, ext):
        result = ProductType.SCIENCE
        obs_intent = self.get_obs_intent(ext)
        if obs_intent == 'calibration':
            result = ProductType.CALIBRATION
        return result

    def get_calibration_level(self, ext):
        cal_level = CalibrationLevel.RAW_STANDARD
        if 'clean' in self._storage_name.file_uri or 'cor' in self._storage_name.file_uri:
            cal_level = CalibrationLevel.CALIBRATED
        return cal_level

    def get_coord1_pix(self, ext):
        ccdsec = self._headers[ext].get('CCDSEC')
        pix = ccdsec.split(',')[0].split(':')[1]
        return pix

    def get_coord2_pix(self, ext):
        ccdsec = self._headers[ext].get('CCDSEC')
        pix = ccdsec.split(',')[1].split(':')[1]
        return pix

    def get_dec(self, ext):
        ra_deg_ignore, dec_deg = self._get_position(ext)
        return dec_deg

    def get_instrument_keywords(self, ext):
        mode = self._get_mode(ext)
        # the blueprint splits for separate keywords on spaces
        result = mode.replace(' ', '')
        return result

    def get_obs_intent(self, ext):
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
        mode = self._get_mode(ext)
        if mode is not None and (
            'FINE_POINT' in mode or 'FINE_SLEW' in mode or 'FINE_HOLD' in mode
        ):
            obs_type = self.get_obs_type(ext)
            if obs_type != 'dark':
                result = 'science'
        return result

    def get_obs_type(self, ext):
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

        mode = self._get_mode(ext)
        if 'DESAT' in mode:
            result = 'dark'
        else:
            obj = self._headers[ext].get('OBJECT')
            if obj is not None and (obj == 'DARK' or obj == 'dark'):
                result = obj.lower()
            else:
                result = 'object'
        return result

    def get_plane_data_release(self, ext):
        result = self._headers[ext].get('RELEASE')
        if result is None:
            result = self._headers[ext].get('DATE-OBS')
        return result

    def get_position_axis_function_naxis1(self, ext):
        result = mc.to_int(self._headers[ext].get('NAXIS1'))
        if result is not None:
            result = result / 2.0
        return result

    def get_position_axis_function_naxis2(self, ext):
        result = mc.to_int(self._headers[ext].get('NAXIS2'))
        if result is not None:
            result = result / 2.0
        return result

    def get_ra(self, ext):
        ra_deg, dec_deg_ignore = self._get_position(ext)
        return ra_deg

    def get_target_moving(self, ext):
        result = True
        moving = self._headers[ext].get('MOVING')
        if moving == 'F':
            result = False
        return result

    def get_target_type(self, ext):
        result = self._headers[ext].get('TARGTYPE')
        if result is not None:
            result = result.lower()
        return result

    def get_time_delta(self, ext):
        result = None
        exptime = mc.to_float(self._headers[ext].get('EXPOSURE'))  # in s
        if exptime is not None:
            result = exptime / (24.0 * 3600.0)
        return result

    def get_time_function_val(self, ext):
        time_string = self._headers[ext].get('DATE-OBS')
        return ac.get_datetime_mjd(time_string)

    def _get_energy(self, ext):
        # DB 24-09-19
        # if bandpass IS None: set min_wl to 0.4, max_wl to 0.9 (microns)
        min_wl = 0.4
        max_wl = 0.9
        # header units are Angstroms
        bandpass = self._headers[ext].get('BANDPASS')
        if bandpass is not None:
            temp = bandpass.split(',')
            min_wl = mc.to_float(temp[0]) / 1e4
            max_wl = mc.to_float(temp[1]) / 1e4
        return min_wl, max_wl

    def _get_mode(self, ext):
        return self._headers[ext].get('MODE')

    def _get_position(self, ext):
        ra = self._headers[ext].get('RA')
        dec = self._headers[ext].get('DEC')
        if ra is None and dec is None:
            # DB 25-09-19
            # Looking at other sample headers for a bunch of files OBJCTRA and
            # OBJCTDEC are always the same as RA and DEC so use those if RA and/or
            # DEC are missing  Note OBJCRA/OBJCTDEC do not have ‘:’ delimiters
            # between hours(degrees)/minutes/seconds.
            ra_temp = self._headers[ext].get('OBJCTRA')
            dec_temp = self._headers[ext].get('OBJCTDEC')
            ra = ra_temp.replace(' ', ':')
            dec = dec_temp.replace(' ', ':')
        ra = None if ra == 'TBD' else ra
        dec = None if dec == 'TBD' else dec
        if ra is not None and dec is not None:
            ra, dec = ac.build_ra_dec_as_deg(ra, dec)
        return ra, dec

    def accumulate_blueprint(self, bp, applicaton=None):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level.

        Guidance for construction is available from this doc:
        https://docs.google.com/document/d/1Z84x9t2iCK72j3-u6LSejYiDi58097oP4PY1up0axjI/edit#
        """
        super().accumulate_blueprint(bp, APPLICATION)

        bp.set('Observation.intent', 'get_obs_intent()')
        # DB 24-09-19
        # If OBSTYPE not in header, set target.type = ‘object’
        # If observation is a 'DARK', set target.type = 'dark'
        bp.set('Observation.type', 'get_obs_type()')
        bp.clear('Observation.instrument.name')
        bp.add_attribute('Observation.instrument.name', 'INSTRUME')
        # DB 24-09-19
        # If INSTRUME not in header, set Observation.instrument.name =
        # ‘NEOSSat_Science’
        bp.set_default('Observation.instrument.name', 'NEOSSat_Science')
        # DB 17-10-19
        # Set Observation.instrument.keywords to the value of the MODE keyword.
        bp.set('Observation.instrument.keywords', 'get_instrument_keywords()')
        bp.set('Observation.target.type', 'get_target_type()')
        bp.set('Observation.target.moving', 'get_target_moving()')
        bp.clear('Observation.proposal.id')
        bp.add_attribute('Observation.proposal.id', 'PROP_ID')
        bp.clear('Observation.proposal.pi')
        bp.add_attribute('Observation.proposal.pi', 'PI_NAME')
        bp.clear('Observation.proposal.title')
        bp.add_attribute('Observation.proposal.title', 'TITLE')

        bp.clear('Plane.metaRelease')
        bp.add_attribute('Plane.metaRelease', 'DATE-OBS')
        bp.set('Plane.dataRelease', 'get_plane_data_release()')
        bp.set('Plane.dataProductType', 'image')
        bp.set('Plane.calibrationLevel', 'get_calibration_level()')
        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'CONV_SW')
        bp.clear('Plane.provenance.version')
        bp.add_attribute('Plane.provenance.version', 'CONV_VER')
        bp.clear('Plane.provenance.producer')
        bp.add_attribute('Plane.provenance.producer', 'CREATOR')
        bp.clear('Plane.provenance.runID')
        bp.add_attribute('Plane.provenance.runID', 'RUNID')

        bp.set('Artifact.releaseType', 'data')
        bp.set('Artifact.productType', 'get_artifact_product_type()')

        bp.configure_time_axis(3)
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.function.naxis', '1')
        bp.set('Chunk.time.axis.function.delta', 'get_time_delta()')
        bp.set('Chunk.time.axis.function.refCoord.pix', '0.5')
        bp.set('Chunk.time.axis.function.refCoord.val', 'get_time_function_val()')
        bp.clear('Chunk.time.exposure')
        bp.add_attribute('Chunk.time.exposure', 'EXPOSURE')

        bp.configure_polarization_axis(5)
        bp.configure_observable_axis(6)

        self._logger.debug('Done accumulate_bp.')

    def update(self, observation, file_info):
        """Called to fill multiple CAOM model elements and/or attributes. """
        self._logger.debug('Begin update.')
        for plane in observation.planes.values():
            if plane.product_id != self._storage_name.product_id:
                continue
            for artifact in plane.artifacts.values():
                if self._storage_name.file_uri == artifact.uri:
                    # TODO why isn't this condition a continue??????
                    update_artifact_meta(artifact, file_info)
                temp_parts = TypedOrderedDict(Part,)
                # need to rename the BINARY TABLE extensions, which have
                # differently telemetry, and remove their chunks
                for part_key in ['1', '2', '3', '4', '5']:
                    if part_key in artifact.parts:
                        hdu_count = mc.to_int(part_key)
                        temp = artifact.parts.pop(part_key)
                        temp.product_type = ProductType.AUXILIARY
                        temp.name = self._headers[hdu_count].get('EXTNAME')
                        while len(temp.chunks) > 0:
                            temp.chunks.pop()
                        temp_parts.add(temp)
                for part in artifact.parts.values():
                    if part.name == '0':
                        part.product_type = artifact.product_type
                        for chunk in part.chunks:
                            chunk.product_type = artifact.product_type
                            self._build_chunk_energy(chunk)
                            self._build_chunk_position(chunk, observation.observation_id)
                            chunk.time_axis = None
                for part in temp_parts.values():
                    artifact.parts.add(part)
        self._logger.debug('Done update.')
        return observation

    def _build_chunk_energy(self, chunk):
        # DB 18-09-19
        # NEOSSat folks wanted the min/max wavelengths in the BANDPASS keyword to
        # be used as the upper/lower wavelengths.  BANDPASS = ‘4000,9000’ so
        # ref_coord1 = RefCoord(0.5, 4000) and ref_coord2 = RefCoord(1.5, 9000).
        # The WAVELENG value is not used for anything since they opted to do it
        # this way.  They interpret WAVELENG as being the wavelengh of peak
        # throughput of the system I think.

        min_wl, max_wl = self._get_energy(0)
        axis = CoordAxis1D(axis=Axis(ctype='WAVE', cunit='um'))
        if min_wl is not None and max_wl is not None:
            ref_coord1 = RefCoord(0.5, min_wl)
            ref_coord2 = RefCoord(1.5, max_wl)
            axis.range = CoordRange1D(ref_coord1, ref_coord2)

            # DB 24-09-19
            # If FILTER not in header, set filter_name = ‘CLEAR’
            filter_name = self._headers[0].get('FILTER', 'CLEAR')

            # DB 24-09-19
            # if wavelength IS None, wl = 0.6 microns, and resolving_power is
            # always determined.
            wavelength = self._headers[0].get('WAVELENG', 6000)
            wl = wavelength / 1e4  # everything in microns
            resolving_power = wl / (max_wl - min_wl)
            energy = SpectralWCS(
                axis=axis,
                specsys='TOPOCENT',
                ssyssrc='TOPOCENT',
                ssysobs='TOPOCENT',
                bandpass_name=filter_name,
                resolving_power=resolving_power,
            )
            chunk.energy = energy

    def _build_chunk_position(self, chunk, obs_id):
        # DB 18-08-19
        # Ignoring rotation for now:  Use CRVAL1 = RA from header, CRVAL2 = DEC
        # from header.  NAXIS1/NAXIS2 values gives number of pixels along RA/DEC
        # axes (again, ignoring rotation) and assume CRPIX1 = NAXIS1/2.0 and
        # CRPIX2 = NAXIS2/2.0 (i.e. in centre of image).   XBINNING/YBINNING
        # give binning values along RA/DEC axes.   CDELT1 (scale in
        # degrees/pixel; it’s 3 arcsec/unbinned pixel)= 3.0*XBINNING/3600.0
        # CDELT2 = 3.0*YBINNING/3600.0.  Set CROTA2=0.0
        header = self._headers[0]
        ra = self.get_ra(0)
        dec = self.get_dec(0)
        if ra is None or dec is None:
            self._logger.warning(f'No position information for {obs_id}')
            chunk.naxis = None
        else:
            header['CTYPE1'] = 'RA---TAN'
            header['CTYPE2'] = 'DEC--TAN'
            header['CUNIT1'] = 'deg'
            header['CUNIT2'] = 'deg'
            header['CRVAL1'] = ra
            header['CRVAL2'] = dec
            header['CRPIX1'] = self.get_position_axis_function_naxis1(0)
            header['CRPIX2'] = self.get_position_axis_function_naxis2(0)

            wcs_parser = FitsWcsParser(header, obs_id, 0)
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
            cd12 = abs(cdelt2) * NEOSSatMapping._sign(cdelt1) * math.sin(crota2_rad)
            cd21 = -abs(cdelt1) * NEOSSatMapping._sign(cdelt2) * math.sin(crota2_rad)
            cd22 = cdelt2 * math.cos(crota2_rad)

            dimension = Dimension2D(header.get('NAXIS1'), header.get('NAXIS2'))
            x = RefCoord(self.get_position_axis_function_naxis1(0), self.get_ra(0))
            y = RefCoord(self.get_position_axis_function_naxis2(0), self.get_dec(0))
            ref_coord = Coord2D(x, y)
            function = CoordFunction2D(dimension, ref_coord, cd11, cd12, cd21, cd22)
            chunk.position.axis.function = function
            chunk.position_axis_1 = 1
            chunk.position_axis_2 = 2

    @staticmethod
    def _sign(value):
        return -1.0 if value < 0.0 else 1.0
