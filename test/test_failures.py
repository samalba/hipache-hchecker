
import os
import time
import signal

import base


class SimpleTestCase(base.TestCase):

    def test_backend_freeze(self):
        """ Monitoring of a frozen backend """
        port = 1080
        pid = self.spawn_httpd(port)
        frontend = self.add_check(port)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 0)
        self.assertEqual(self.http_request(port), 200)

        # Freezing the backend
        os.kill(pid, signal.SIGSTOP)
        time.sleep(8)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 1)
        self.assertEqual(self.http_request(port), -1)

        # Unfreezing the backend
        os.kill(pid, signal.SIGCONT)
        time.sleep(4)
        dead = self.redis.smembers('dead:{0}'.format(frontend))
        self.assertEqual(len(dead), 0)
        self.assertEqual(self.http_request(port), 200)
