
import time

import base


class SimpleTestCase(base.TestCase):

    def test_simple(self):
        """ Monitoring of a valid HTTP server """
        port = 1080
        self.spawn_httpd(port)
        frontend = self.add_check(port)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 0)
        self.assertEqual(self.http_request(port), 200)

    def test_single_backend(self):
        """ Monitoring of a single HTTP backend (not checked) """
        port = 1080
        self.spawn_httpd(port, 501)
        # This check should be ignored
        frontend = self.add_check(port, num_backends=1)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 0)
        self.assertEqual(self.http_request(port), 501)

    def test_error(self):
        """ Monitoring of an invalid HTTP server """
        port = 1080
        frontend = self.add_check(port)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 1)
        self.assertEqual(self.http_request(port), -1)

    def test_multiple_backends(self):
        """ Monitoring of a frontend with 2 backends """
        port = 1080
        self.spawn_httpd(port, 501)
        self.spawn_httpd(port + 1, 200)
        frontend = self.add_check(port)
        self.add_check(port + 1, frontend=frontend)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 1)
        self.assertEqual(self.http_request(port), 501)
        self.assertEqual(self.http_request(port + 1), 200)
