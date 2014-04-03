#
# Unit tests for Server class
#


TEST_CONNECT_DBNAME = "postgres"

class TestServerClass(unittest.TestCase):

    def test_connect(self):
        "Test pscopg2 connection to the server"

        try:
            test_server().raw_connect_to(TEST_CONNECT_DBNAME,
                                         test_server().user)
        except psycopg2.Error as err:
            self.fail("Unable to connect as user '%s'" % test_server().user +
                      ((":\n" + err.pgerr) if err.pgerr else ""))

        try:
            test_server().raw_connect_to(TEST_CONNECT_DBNAME,
                                       test_server().superuser)
        except psycopg2.Error as err:
            self.fail("Unable to connect as superuser '%s'" %
                      test_server().superuser +
                      ((":\n" + err.pgerr) if err.pgerr else ""))



#
# Define test suites
#

def the_suite():
    """Returns a test suite of all the tests in this module."""

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestServerClass)
    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
