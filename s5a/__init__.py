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
from h3 import h3


# Logger
logger = logging.getLogger(__name__)


def load_ncfile(ncfile, data_variable_name=None):
    """Load a ncfile into a pandas dataframe

    :param ncfile: path of the file to be read
    :type ncfile: string
    :data_variable_name: the name of the data variable in the
        product to load, defaults to None
    :type data_variable_name: string
    :return: a pandas dataframe containing
        the groundpixel information as points
    :rtype: pandas.core.frame.DataFrame
    """

    # read in data
    with netCDF4.Dataset(ncfile, 'r') as f:
        variables = f.groups['PRODUCT'].variables

        # If no data variable name is given, choose the first one
        # that is available from the following lookup table.
        variable_names = ('carbonmonoxide_total_column',
                          'ozone_total_vertical_column',
                          'sulfurdioxide_total_vertical_column',
                          'methane_mixing_ratio_bias_corrected',
                          'formaldehyde_tropospheric_vertical_column',
                          'nitrogendioxide_tropospheric_column'
                          )

        if not data_variable_name:
            data_variable_name = (variables.keys() & variable_names).pop()

        data = variables[data_variable_name][:][0]
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
    if len(deltatime.shape) == 1:
        deltatime = numpy.repeat(
            deltatime, pixel_per_line).reshape(n_lines, -1)
    deltatime = deltatime[mask]  # filter for missing data
    timestamps = pandas.to_datetime(deltatime, utc=True, unit='ms')

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
    :type dataframe: pandas.core.frame.DataFrame
    :param minimal_quality: Minimal allowed quality,
        has to be in the range of 0.0 - 1.0 and defaults to
        0.5 as suggested by the ESA product manual
    :type minimal_quality: float
    :return: the dataframe filtered by the specified value
    :rtype: pandas.core.frame.DataFrame
    """
    has_quality = dataframe.quality >= minimal_quality
    return dataframe[has_quality]


def point_to_h3(dataframe, resolution=1):
    """Convert longitude and latitude in pandas dataframe into h3 indices and
    add them as additional column.

    :param dataframe: a pandas dataframe as returned from load_ncfile()
    :type dataframe: pandas.core.frame.DataFrame
    :param resolution: Resolution of the h3 grid
    :type resolution: uint
    :return: the dataframe including the h3 indices
    :rtype: pandas.core.frame.DataFrame
    """

    # create a new column 'h3' and fill it row-wise with
    # the converted longitudes and latitudes
    dataframe['h3'] = [h3.geo_to_h3(lat, lon, resolution)
                       for lat, lon in
                       zip(dataframe['latitude'], dataframe['longitude'])]

    return dataframe


def h3_to_point(dataframe, h3_column='h3'):
    """Convert H3 index in pandas dataframe into longitude and latitude.

    :param: dataframe: pandas dataframe with a column of H3 indices
    :type dataframe: pandas.core.frame.DataFrame
    :param h3_column: column name of the column with the H3 indices,
                      defaults to h3.
    :type h3_column: str, optional
    """
    lat_lon = numpy.array(
        [h3.h3_to_geo(h3hexagon) for h3hexagon in dataframe['h3']])
    dataframe['latitude'] = lat_lon[:, 0]
    dataframe['longitude'] = lat_lon[:, 1]
    return dataframe


def aggregate_h3(dataframe, function='mean'):
    """Aggregate data values of the same H3 index in dataframe.

    :param dataframe: a pandas dataframe as returned from load_ncfile()
    :type dataframe: pandas.core.frame.DataFrame
    :param function: Aggregation function of
        the data values of the same h3 index
    :type function: str
    :return: new dataframe with the aggregated values
    :rtype: pandas.core.frame.DataFrame
    """

    if function not in ['median', 'mean']:
        raise ValueError("invalid parameter for function")

    # aggregate same indices
    return dataframe.groupby(['h3'], as_index=False).agg({
        'timestamp': 'min',
        'quality': 'min',
        'value': function})
