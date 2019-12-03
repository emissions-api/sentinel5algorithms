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
import itertools
import statistics
import numpy

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


def point_to_h3(dataframe, resolution=1):
    """Convert pandas dataframe into h3 indices.

    :param dataframe: a pandas dataframe as returned from load_ncfile()
    :type dataframe: pandas.GeoDataFrame
    :param resolution: Resolution of the h3 grid
    :type resolution: uint
    :return: the dataframe including the h3 indices
    :rtype: pandas.GeoDataFrame
    """

    h3_series = []

    # go through all points in scan
    for point in range(len(dataframe)):

        print("point#: ", point)

        # skip point if value is nan
        if not numpy.isnan(dataframe.value[point]):

            # convert points (coordinates) into h3 grid (hexagon index)
            h3_index = h3.geo_to_h3(dataframe.longitude[point], dataframe.latitude[point], resolution)

            print("h3_index: ", h3_index)

            h3_series.append(h3_index)

    print("h3_series: ", h3_series)

    dataframe['h3'] = h3_series

    print("merged_dataframe: ", dataframe)

    return dataframe


def aggregate_h3(dataframe, function=['median', 'mean']):
    """Sum up data values of the same h3 index in dataframe.

    :param dataframe: a geopanda dataframe as returned from load_ncfile()
    :type dataframe: pandas.GeoDataFrame
    :param function: Sum up function of the data values of the same h3 index
    :type function:
    :return: the dataframe including the h3 indices
    :rtype: pandas.GeoDataFrame
    """

    # set first element of the list as grouping key
    def extract_key(v):
        return v[0]

    # itertools.groupby needs data to be sorted first
    self.h3_indices = sorted(self.h3_indices, key=extract_key)

    # group all values of identical indices in seperate groups
    self.h3_grouped = [
        [key, [x[1] for x in group]]
        for key, group in itertools.groupby(self.h3_indices, extract_key)
    ]

    # go through all grouped h3 index values of each group
    for row in range(len(self.h3_grouped)):
        for value in range(len(self.h3_grouped[row])):

            # TODO: delete
            print('h3_grouped[', row, '][', value, ']: ',
                    self.h3_grouped[row][value])

        # overwrite first value of each group with the median value of the group
        self.h3_grouped[row][1] = statistics.median(
            self.h3_grouped[row][1])

        # TODO: delete
        print('median: ', self.h3_grouped[row][1])

    # TODO: delete
    for row in range(len(self.h3_grouped)):
        for value in range(len(self.h3_grouped[row])):
            print('median_list[', row, '][', value, ']: ',
                    self.h3_grouped[row][value])

    return dataframe
