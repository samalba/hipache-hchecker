
import time

import base


class SimpleTestCase(base.TestCase):

    def test_codes(self):
        """ Test all codes """

        def check_code(code, expect):
            """ `code' is the code returned by the server (-1 is timeout).
                `expect' is the number of dead backend expected in the Redis.
            """
            port = 1080
            if code > 0:
                pid = self.spawn_httpd(port, code)
            frontend = self.add_check(port)
            time.sleep(4)
            dead = self.redis.smembers('dead:{0}'.format(frontend))
            self.assertEqual(len(dead), expect, '{0} != {1} (code = {2})'.format(
                len(dead), expect, code))
            self.assertEqual(self.http_request(port), code)
            if code > 0:
                self.stop_httpd(pid)
            return len(dead)

        check_code(200, 0)
        check_code(302, 0)
        check_code(404, 0)
        check_code(500, 1)
        check_code(501, 1)
        check_code(502, 1)
        check_code(503, 0)
        # No server behind
        check_code(-1, 1)
