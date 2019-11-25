# Copyright 2019, The Emissions API Developers
# https://emissions-api.org
# This software is available under the terms of an MIT license.
# See LICENSE fore more information.
"""Preprocess the locally stored data and store them in the database.
"""
import logging

import geopandas
import netCDF4
import numpy
import pandas

# Logger
logger = logging.getLogger(__name__)


def load_ncfile(ncfile):

    # read in data
    with netCDF4.Dataset(ncfile, 'r') as f:
        variables = f.groups['PRODUCT'].variables
        data = variables['carbonmonoxide_total_column'][:][0]
        longitude = variables['longitude'][:][0]
        latitude = variables['latitude'][:][0]
        quality = variables['qa_value'][:][0]
        deltatime = variables['delta_time'][:][0]
        meta_data = f.__dict__

    # get some metadata
    # use mask from MaskedArray to filter values
    mask = numpy.logical_not(data.mask)
    n_lines = data.shape[0]  # number of scan lines
    pixel_per_line = data.shape[1]  # number of pixels per line
    time_reference = meta_data['time_reference_seconds_since_1970']

    # convert deltatime to timestamps
    deltatime_arr = numpy.repeat(
        deltatime, pixel_per_line).reshape(n_lines, -1)
    deltatime_arr = deltatime_arr[mask]  # filter for missing data
    # add (milli-)seconds since 1970
    deltatime_arr = numpy.add(deltatime_arr, time_reference * 1000)
    timestamps = pandas.to_datetime(deltatime_arr, utc=True, unit='ms')

    # convert data to geodataframe
    return geopandas.GeoDataFrame({
        'timestamp': timestamps,
        'quality': quality[mask],
        'data': data[mask]
    },
        geometry=geopandas.points_from_xy(
            longitude[mask],
            latitude[mask])
    )


class Scan():
    """Object to hold arrays from an nc file.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = load_ncfile(filepath)

    def filter_by_quality(self, minimal_quality):
        """Filter points of the Scan by quality.

        :param minimal_quality: Minimal allowed quality
        :type minimal_quality: int
        """
        has_quality = self.data.quality >= minimal_quality
        self.data = self.data[has_quality]

    def len(self):
        """Get number of points in Scan.

        :return: Number of points
        :rtype: int
        """
        return self.data.shape[0]
