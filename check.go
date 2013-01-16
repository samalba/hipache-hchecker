
package main

import (
    "fmt"
    "log"
    "time"
    "errors"
    "strings"
    "strconv"
    "net"
    "net/http"
)

const (
    // The HTTP method used for each test
    HTTP_METHOD = "HEAD"
    // Check the URL every 3 seconds
    CHECK_INTERVAL = 3
    // If the test keeps the same state for 1 hour, stop it
    CHECK_DURATION = 3600
    // Check every 1 minute if we break the check
    CHECK_BREAK_INTERVAL = 60
    // Connection timeout is 3 seconds by default
    CONNECTION_TIMEOUT = 3
    // IO timeout applies after the connection
    IO_TIMEOUT = 3
    // User-Agent for all tests
    USER_AGENT = "dotCloud-HealthCheck/1.0 go/1.0.3"
)

var (
    httpClient *http.Client
    httpMethod string
    checkInterval time.Duration
    checkDuration = time.Duration(CHECK_DURATION) * time.Second
    checkBreakInterval = time.Duration(CHECK_BREAK_INTERVAL) * time.Second
    connectionTimeout time.Duration
    ioTimeout time.Duration
    userAgent string
)

type Check struct {
    BackendUrl string
    BackendId int
    BackendGroupLength int
    FrontendKey string

    // Goroutine unique signature
    routineSig string

    // Called when backend dies
    deadCallback func ()
    // Called when the backend comes back to life
    aliveCallback func ()
    // Called every CHECK_BREAK_INTERVAL to stop the routine if returned true
    checkIfBreakCallback func () bool
    // Called when the check exits
    exitCallback func ()
}

func NewCheck(line string) (*Check, error) {
    parts := strings.Split(strings.TrimSpace(line), ";")
    if len(parts) != 4 {
        return nil, errors.New("Invalid check line")
    }
    backendId, _ := strconv.Atoi(parts[2])
    backendGroupLength, _ := strconv.Atoi(parts[3])
    c := &Check{BackendUrl: parts[1], BackendId: backendId,
        BackendGroupLength: backendGroupLength, FrontendKey: parts[0]}
    return c, nil
}

func createHttpTransport() (*http.Transport) {
    httpDial := func (proto string, addr string) (net.Conn, error) {
        conn, err := net.DialTimeout(proto, addr, connectionTimeout)
        if err != nil {
            return nil, err
        }
        conn.SetDeadline(time.Now().Add(ioTimeout))
        return conn, nil
    }
    return &http.Transport{
        DisableKeepAlives: true,
        Dial: httpDial,
    }
}

func (c *Check) SetDeadCallback(callback func ()) {
    c.deadCallback = callback
}

func (c *Check) SetAliveCallback(callback func ()) {
    c.aliveCallback = callback
}

func (c *Check) SetCheckIfBreakCallback(callback func () bool) {
    c.checkIfBreakCallback = callback
}

func (c *Check) SetExitCallback(callback func ()) {
    c.exitCallback = callback
}

func (c* Check) PingUrl() {
    // Current status, true for alive, false for dead
    var (
        testError string
        lastDeadCall time.Time
        lastStateChange = time.Now()
        status = true
        newStatus = true
        i = time.Duration(0)
        )
    if httpClient == nil {
        httpClient = &http.Client{Transport: createHttpTransport()}
    }
    for {
        req, _ := http.NewRequest(httpMethod, c.BackendUrl, nil)
        req.Header.Add("User-Agent", userAgent)
        resp, err := httpClient.Do(req)
        if err != nil {
            // TCP error
            newStatus = false
            testError = fmt.Sprintf("TCP error on %s: %#v",
                c.BackendUrl, err.Error())
        } else {
            // No TCP error, checking HTTP code
            if resp.StatusCode >= 500 && resp.StatusCode < 600 &&
                resp.StatusCode != 503 {
                    newStatus = false
                    testError = fmt.Sprintf("HTTP error on %s: %#v",
                        c.BackendUrl, resp.StatusCode)
                } else {
                    newStatus = true
                }
        }
        // Check if the status changed before updating Redis
        if newStatus != status {
            lastStateChange = time.Now()
            if newStatus == true {
                log.Printf("%s is back online\n", c.BackendUrl)
                if c.aliveCallback != nil {
                    c.aliveCallback()
                }
                lastDeadCall = time.Time{}
            } else {
                log.Println(testError)
                if c.deadCallback != nil {
                    c.deadCallback()
                }
                lastDeadCall = time.Now()
            }
        } else if newStatus == false {
            // Backend is still dead. Mark it as dead every 30 seconds to keep
            // it dead despite the Redis TTL
            if lastDeadCall.IsZero() == false &&
                time.Since(lastDeadCall) >=
                (time.Duration(30) * time.Second) {
                    if c.deadCallback != nil {
                        c.deadCallback()
                    }
                    lastDeadCall = time.Now()
                }
        }
        status = newStatus
        time.Sleep(checkInterval)
        i += checkInterval
        // At longer interval, we check if still have the lock on the backend
        if i >= checkBreakInterval {
            if c.checkIfBreakCallback != nil &&
                c.checkIfBreakCallback() == true {
                    log.Printf("Lost the lock for %s\n", c.BackendUrl)
                    break
            }
            // Let's see if the check is in the same state for a while
            if time.Since(lastStateChange) >= checkDuration {
                log.Printf("%s stayed in the same state for a long time\n",
                    c.BackendUrl)
                break
            }
            i = time.Duration(0)
        }
    }
    if c.exitCallback != nil {
        c.exitCallback()
    }
}
