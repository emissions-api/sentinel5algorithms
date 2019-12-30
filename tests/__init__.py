import os.path
import unittest

import s5a

TEST_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'test.nc')


class TestS5a(unittest.TestCase):
    def setUp(self):
        self.data = s5a.load_ncfile(TEST_FILE)
        self.assertEqual(self.data.size, 1130)

    def test_quality(self):
        # tuples of quality and number of points
        tests = (
            (0, 1130),
            (0.5, 900),
            (1, 0),
        )
        for quality, expected_results in tests:
            self.assertEqual(s5a.filter_by_quality(
                self.data, quality).size, expected_results)

    def test_h3(self):
        d = s5a.point_to_h3(self.data)
        self.assertIn('h3', d.columns)

        self.assertEqual(s5a.aggregate_h3(d).size, 12)
