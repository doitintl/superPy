# import the package
from superQuery import *
import unittest

class SuperTests(unittest.TestCase):
    def test_clean_stats(self):

        sq_client = Client()

        # If we get a string
        stats = { 'superQueryTotalBytesProcessed': 'None' }
        stats = sq_client.clean_stats(stats)
        self.assertEqual(stats['superQueryTotalBytesProcessed'], 0)

        # If we get a float
        stats = { 'superQueryTotalBytesProcessed': 0.0 }
        stats = sq_client.clean_stats(stats)
        self.assertEqual(stats['superQueryTotalBytesProcessed'], 0)

        # If we get an integer
        stats = { 'superQueryTotalBytesProcessed': 0 }
        stats = sq_client.clean_stats(stats)
        self.assertEqual(stats['superQueryTotalBytesProcessed'], 0)