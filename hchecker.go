
package main

import (
        "os"
        "fmt"
        "log"
        "time"
        "runtime"
        )

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

func main() {
    var (
        err error
        hostname string
        )
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
