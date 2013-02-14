
import sys
import time
import redis


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: {0} <number of backends> [add interval (seconds)]' \
                .format(sys.argv[0])
        sys.exit(0)
    n = int(sys.argv[1])
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else 0
    r = redis.StrictRedis()
    r.delete('hchecker')
    for i in range(n):
        line = 'frontend{0};http://localhost:{1};0;2'.format(i, 4242 + i)
        interval > 0 and time.sleep(interval)
        r.publish('dead', line)
