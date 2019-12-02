# Copyright 2019, The Emissions API Developers
# https://emissions-api.org
# This software is available under the terms of an MIT license.
# See LICENSE fore more information.
"""Preprocess the locally stored data and store them in the database.
"""
import logging

import netCDF4
import numpy
import pandas

# Logger
logger = logging.getLogger(__name__)


def load_ncfile(ncfile):
    """Load a ncfile into a pandas dataframe

    :param ncfile: path of the file to be read
    :type ncfile: string
    :return: a pandas dataframe containing
        the groundpixel information as points
    :rtype: pandas.DataFrame
    """

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
    # add (milli-)seconds since 1970
    deltatime = numpy.add(deltatime, time_reference * 1000)
    deltatime_arr = numpy.repeat(
        deltatime, pixel_per_line).reshape(n_lines, -1)
    deltatime_arr = deltatime_arr[mask]  # filter for missing data
    timestamps = pandas.to_datetime(deltatime_arr, utc=True, unit='ms')

    # convert data to geodataframe
    return pandas.DataFrame({
            'timestamp': timestamps,
            'quality': quality[mask],
            'value': data[mask],
            'longitude': longitude[mask],
            'latitude': latitude[mask]
        })


def filter_by_quality(dataframe, minimal_quality=0.5):
    """Filter points by quality.

    :param dataframe: a dataframe as returned from load_ncfile()
    :type dataframe: pandas.DataFrame
    :param minimal_quality: Minimal allowed quality,
        has to be in the range of 0.0 - 1.0 and defaults to
        0.5 as suggested by the ESA product manual
    :type minimal_quality: float
    :return: the dataframe filtered by the specified value
    :rtype: pandas.DataFrame
    """
    has_quality = dataframe.quality >= minimal_quality
    return dataframe[has_quality]
