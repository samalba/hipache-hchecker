
import os
import time
import signal
import logging
import unittest
import BaseHTTPServer

import redis
import requests


logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO)


class HTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_GET(self):
        self.send_response(self.server.code)
        self.send_header('Content-Length', '0')
        self.send_header('Connection', 'close')
        self.end_headers()

    def do_HEAD(self):
        return self.do_GET()


class TestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.redis = redis.StrictRedis()
        self.check_ready()
        self._httpd_pids = []
        self.addCleanup(self.stop_all_httpd)

    def check_ready(self):
        """ Makes sure the activechecker is running """
        ping = self.redis.get('hchecker_ping')
        if not ping or (int(time.time()) - int(ping)) > 30:
            self.fail('hchecker is not running (Please launch the hchecker '
                    'manually before starting the tests)')

    def stop_all_httpd(self):
        if not self._httpd_pids:
            return
        for pid in self._httpd_pids:
            os.kill(pid, signal.SIGKILL)
            logger.info('httpd killed. PID: {0}'.format(pid))
        os.wait()

    def spawn_httpd(self, port, code=200):
        pid = os.fork()
        if pid > 0:
            # In the father, wait for the child to be available
            while True:
                r = self.http_request(port)
                if r > 0:
                    self._httpd_pids.append(pid)
                    logger.info('httpd spawned on port {0}. PID: {1}'.format(port, pid))
                    return pid
                time.sleep(0.5)
        # In the child, spawn the httpd
        httpd = BaseHTTPServer.HTTPServer(('localhost', port), HTTPHandler)
        httpd.code = code
        httpd.serve_forever()

    def stop_httpd(self, pid):
        os.kill(pid, signal.SIGKILL)
        logger.info('httpd stopped. PID: {0}'.format(pid))
        os.wait()
        if pid in self._httpd_pids:
            self._httpd_pids.remove(pid)

    def register_frontend(self, frontend, backend_url):
        self.redis.rpush('frontend:{0}'.format(frontend), frontend, backend_url)

    def unregister_frontend(self, frontend):
        self.redis.delete('frontend:{0}'.format(frontend))
        self.redis.delete('dead:{0}'.format(frontend))

    def add_check(self, port, frontend=None, num_backends=2):
        if not frontend:
            frontend = 'frontend-{0:6f}'.format(time.time())
        if not isinstance(port, (tuple, list)):
            port = [port]
        self.unregister_frontend(frontend)
        for p in port:
            self.register_frontend(frontend, 'http://localhost:{0}'.format(p))
            line = '{0};http://localhost:{1};0;{2}'.format(frontend, p,
                    num_backends)
            self.redis.publish('dead', line)
        return frontend

    def http_request(self, port):
        try:
            r = requests.get('http://localhost:{0}/'.format(port), timeout=1.0)
            return r.status_code
        except (requests.ConnectionError, requests.Timeout):
            return -1
