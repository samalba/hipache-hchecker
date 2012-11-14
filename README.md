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
      -agent="dotCloud-HealthCheck/1.0 go/1.0.3": HTTP User-Agent header
      -connect=3: TCP connection timeout (seconds)
      -interval=3: Check interval (seconds)
      -io=3: HTTP read/write timeout (seconds)
      -method="HEAD": HTTP method
