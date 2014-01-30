Hipache Health-Checker
======================

This is an active health-checker for the Hipache proxy solution. The checker
will check the backends at a regular interval and it will update Hipache's
Redis to mark them as dead (or alive) almost instantly.

1. Compile
----------

    go build

2. Run it
---------

    ./hchecker

It connects on the local redis (localhost:6379), so it's supposed to be run
on the same machine than Hipache.

3. Modify the behavior
----------------------

    ./hchecker -h
    Usage of ./hchecker:
      -connect=3: TCP connection timeout (seconds)
      -cpuprofile=false: Write CPU profile to "hchecker.prof" (current directory)
      -dryrun=false: Enable dry run (or simulation mode). Do not update the Redis.
      -host="ping": HTTP host header
      -interval=3: Check interval (seconds)
      -io=3: Socket read/write timeout (seconds)
      -method="HEAD": HTTP method
      -redis="localhost:6379": Network address of Redis
      -redis_password="": Password of Redis
      -uri="/CloudHealthCheck": HTTP URI

4. Run the tests
----------------

    $ cd test ; python -m unittest discover
