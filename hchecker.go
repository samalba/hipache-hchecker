
package main

import (
        "os"
        "fmt"
        "log"
        "time"
        "flag"
        "runtime"
        )

const VERSION = "0.1.1"

var (
    myId string
    cache *Cache
    runningCheckers = 0
)

func addCheck(line string) {
    check, err := NewCheck(line)
    if err != nil {
        log.Printf("Warning: got invalid data on the \"dead\" " +
            "channel (\"%s\")\n", line)
        return
    }
    if check.BackendGroupLength <= 1 {
        // Add the check only if the frontend is scaled to several
        // backends (backend is part of a group)
        return
    }
    // Set all the callbacks for the check. They will be called during
    // the PingUrl at several steps
    check.SetDeadCallback(func () {
        cache.MarkBackendDead(check)
    })
    check.SetAliveCallback(func () {
        cache.MarkBackendAlive(check)
    })
    check.SetCheckIfBreakCallback(func () bool {
        return cache.IsUnlockedBackend(check)
    })
    check.SetExitCallback(func () {
        runningCheckers -= 1
    })
    r := cache.LockBackend(check)
    if r == false {
        // Backend already locked
        return
    }
    // Check the URL at a regular interval
    go check.PingUrl()
    runningCheckers += 1
    log.Printf("Added check for: %s\n", check.BackendUrl)
}

/*
 * Prints some stats on runtime
 */
func printStats() {
    go func () {
        for {
            log.Printf("%d backend URLs are being tested\n", runningCheckers)
            time.Sleep(time.Duration(1) * time.Minute)
        }
    }()
}

func parseFlags() {
    parseDuration := func (v *time.Duration, n string, def int, help string) {
        i := flag.Int(n, def, help)
        *v = time.Duration(*i) * time.Second
    }
    flag.StringVar(&httpMethod, "method", HTTP_METHOD,
        "HTTP method")
    parseDuration(&checkInterval, "interval", CHECK_INTERVAL,
        "Check interval (seconds)")
    parseDuration(&connectionTimeout, "connect", CONNECTION_TIMEOUT,
        "TCP connection timeout (seconds)")
    parseDuration(&ioTimeout, "io", IO_TIMEOUT,
        "Socket read/write timeout (seconds)")
    flag.StringVar(&userAgent, "agent", USER_AGENT,
        "HTTP User-Agent header")
    flag.StringVar(&redisAddress, "redis", REDIS_ADDRESS,
        "Network address of Redis")
    flag.Parse()
}

func main() {
    var (
        err error
        hostname string
        )
    for  _, arg := range os.Args {
        if !(arg == "-v" || arg == "--version" || arg == "-version") {
            continue
        }
        fmt.Println("hchecker version", VERSION)
        os.Exit(0)
    }
    parseFlags()
    runtime.GOMAXPROCS(runtime.NumCPU())
    hostname, _ = os.Hostname()
    myId = fmt.Sprintf("%s#%d", hostname, os.Getpid())
    // Prefix each line of log
    log.SetPrefix(myId + " ")
    cache, err = NewCache()
    if err != nil {
        log.Println(err.Error())
        os.Exit(1)
    }
    printStats()
    err = cache.ListenToChannel("dead", addCheck)
    if err != nil {
        log.Println(err.Error())
    }
}
