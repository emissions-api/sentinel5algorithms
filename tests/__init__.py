import os.path
import unittest

import s5a

TEST_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'test.nc')


class TestS5a(unittest.TestCase):
    def setUp(self):
        self.data = s5a.load_ncfile(TEST_FILE)
        self.assertEqual(len(self.data), 226)

    def test_quality(self):
        # tuples of quality and number of points
        tests = (
            (0, 226),
            (0.5, 180),
            (1, 0),
        )
        for quality, expected_results in tests:
            self.assertEqual(len(s5a.filter_by_quality(
                self.data, quality)), expected_results)

    def test_h3(self):
        # Create H3 indices
        d = self.data.copy()
        d = s5a.point_to_h3(d)
        self.assertIn('h3', d.columns)

        # Aggregate
        self.assertEqual(len(s5a.aggregate_h3(d)), 5)

        keys = ['longitude', 'latitude']

        # Check if all longitudes and latitudes are updated
        d = s5a.h3_to_point(d)
        unchanged = (d[keys] - self.data[keys] == 0).sum()
        for key in keys:
            self.assertEqual(unchanged[key], 0)

        # Check if longitude and latitude are added if not exist
        d = d.drop(columns=keys)
        d = s5a.h3_to_point(d)
        for key in keys:
            self.assertIn(key, d.columns)
