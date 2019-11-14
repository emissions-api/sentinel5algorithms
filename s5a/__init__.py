# Copyright 2019, The Emissions API Developers
# https://emissions-api.org
# This software is available under the terms of an MIT license.
# See LICENSE fore more information.
"""Preprocess the locally stored data and store them in the database.
"""
import logging

import netCDF4
from datetime import timedelta, datetime

# Logger
logger = logging.getLogger(__name__)


class RawData():
    """Object to hold the raw data from the nc file.
    """

    def __init__(self, ncfile):
        """Load data from netCDF file
        """
        with netCDF4.Dataset(ncfile, 'r') as f:
            variables = f.groups['PRODUCT'].variables
            self.data = variables['carbonmonoxide_total_column'][:][0]
            self.longitude = variables['longitude'][:][0]
            self.latitude = variables['latitude'][:][0]
            self.quality = variables['qa_value'][:][0]
            self.deltatime = variables['delta_time'][:][0]
            self.meta_data = f.__dict__


class Point():
    """Represents a single point with data from the Satellite"""
    def __init__(self, longitude, latitude, value, timestamp, quality):
        self.longitude = longitude
        self.latitude = latitude
        self.value = value
        self.timestamp = timestamp
        self.quality = quality

    def __repr__(self):
        return (
            f'longitude={self.longitude} latitude={self.latitude} '
            f'value={self.value} timestamp={self.timestamp} '
            f'quality={self.quality}')


class Scan():
    """Object to hold arrays from an nc file.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.raw_data = RawData(filepath)
        self.time_reference = datetime.utcfromtimestamp(
            self.raw_data.meta_data['time_reference_seconds_since_1970'])
        self.points = []
        shape = self.raw_data.data.shape
        for i in range(shape[0]):
            for j in range(shape[1]):
                timestamp = self.time_reference + timedelta(
                        milliseconds=int(self.raw_data.deltatime[i]))
                self.points.append(Point(
                    longitude=float(self.raw_data.longitude[i, j]),
                    latitude=float(self.raw_data.latitude[i, j]),
                    value=float(self.raw_data.data[i, j]),
                    quality=float(self.raw_data.quality[i, j]),
                    timestamp=timestamp,
                ))

    def filter_by_quality(self, minimal_quality):
        """Filter points of the Scan by quality.

        :param minimal_quality: Minimal allowed quality
        :type minimal_quality: int
        """
        self.points = [p for p in self.points if p.quality >= minimal_quality]

    def len(self):
        """Get number of points in Scan.

        :return: Number of points
        :rtype: int
        """
        return len(self.points)
