# import the package
from superQuery import *
import unittest

#
# Unit testing for superPy.
#

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

    def test_print_log_query_stats(self):
        sq_client = Client()
        sq_client.set_log_query_results(False)
        self.assertEqual(sq_client.get_log_query_results(), False)
