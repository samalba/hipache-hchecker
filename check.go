package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// The HTTP method used for each test
	HTTP_METHOD = "HEAD"
	// The HTTP URI
	HTTP_URI = "/CloudHealthCheck"
	// HTTP Host header
	HTTP_HOST = "ping"
	// Check the URL every 3 seconds
	CHECK_INTERVAL = 3
	// If the test keeps the same state for 30 min, stop it
	CHECK_DURATION = 1800
	// Check every 1 minute if we break the check
	CHECK_BREAK_INTERVAL = 60
	// Connection timeout is 3 seconds by default
	CONNECTION_TIMEOUT = 3
	// IO timeout applies after the connection
	IO_TIMEOUT = 3
)

var (
	httpTransport      *http.Transport
	httpMethod         string
	httpUri            string
	httpHost           string
	httpUserAgent      string
	checkInterval      time.Duration
	checkDuration      = time.Duration(CHECK_DURATION) * time.Second
	checkBreakInterval = time.Duration(CHECK_BREAK_INTERVAL) * time.Second
	connectionTimeout  time.Duration
	ioTimeout          time.Duration
)

type Check struct {
	BackendUrl         string
	BackendId          int
	BackendGroupLength int
	FrontendKey        string

	// Goroutine unique signature
	routineSig string

	// Called when backend dies
	deadCallback func()
	// Called when the backend comes back to life
	aliveCallback func()
	// Called every CHECK_BREAK_INTERVAL to stop the routine if returned true
	checkIfBreakCallback func() bool
	// Called when the check exits
	exitCallback func()
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
	if len(httpUserAgent) == 0 {
		httpUserAgent = fmt.Sprintf("dotCloud-HealthCheck/%s %s", VERSION,
			runtime.Version())
	}
	return c, nil
}

func (c *Check) SetDeadCallback(callback func()) {
	c.deadCallback = callback
}

func (c *Check) SetAliveCallback(callback func()) {
	c.aliveCallback = callback
}

func (c *Check) SetCheckIfBreakCallback(callback func() bool) {
	c.checkIfBreakCallback = callback
}

func (c *Check) SetExitCallback(callback func()) {
	c.exitCallback = callback
}

func (c *Check) doHttpRequest() (*http.Response, error) {
	if httpTransport == nil {
		httpDial := func(proto string, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(proto, addr, connectionTimeout)
			if err != nil {
				return nil, err
			}
			conn.SetDeadline(time.Now().Add(ioTimeout))
			return conn, nil
		}
		httpTransport = &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
			Dial:               httpDial,
		}
	}
	req, _ := http.NewRequest(httpMethod, c.BackendUrl, nil)
	req.URL.Path = httpUri
	req.Host = httpHost
	req.Header.Add("User-Agent", httpUserAgent)
	req.Close = true
	return httpTransport.RoundTrip(req)
}

func (c *Check) PingUrl(ch chan int) {
	// Current status, true for alive, false for dead
	var (
		lastDeadCall    time.Time
		lastStateChange = time.Now()
		status          = false
		newStatus       = true
		firstCheck      = true
		i               = time.Duration(0)
	)
	for {
		select {
		case <-ch:
			// If we added a frontend to the mapping, we consider it's the
			// first check
			firstCheck = true
		default:
		}
		resp, err := c.doHttpRequest()
		if err != nil {
			// TCP error
			newStatus = false
			log.Println(c.BackendUrl, "TCP error:", err.Error())
		} else {
			// No TCP error, checking HTTP code
			if resp.StatusCode >= 500 && resp.StatusCode < 600 &&
				resp.StatusCode != 503 {
				newStatus = false
				log.Println(c.BackendUrl, "HTTP error:", resp.Status)
			} else {
				newStatus = true
				log.Println(c.BackendUrl, "OK", resp.StatusCode)
			}
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		// Check if the status changed before updating Redis
		if newStatus != status || firstCheck == true {
			lastStateChange = time.Now()
			if newStatus == true {
				if c.aliveCallback != nil {
					c.aliveCallback()
				}
				lastDeadCall = time.Time{}
			} else {
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
					(time.Duration(30)*time.Second) {
				if c.deadCallback != nil {
					c.deadCallback()
				}
				lastDeadCall = time.Now()
			}
		}
		status = newStatus
		firstCheck = false
		time.Sleep(checkInterval)
		i += checkInterval
		// At longer interval, we check if still have the lock on the backend
		if i >= checkBreakInterval {
			if c.checkIfBreakCallback != nil &&
				c.checkIfBreakCallback() == true {
				log.Println(c.BackendUrl, "Lost the lock")
				break
			}
			// Let's see if the check is in the same state for a while
			if time.Since(lastStateChange) >= checkDuration {
				log.Println(c.BackendUrl, "Removed check")
				break
			}
			i = time.Duration(0)
		}
	}
	if c.exitCallback != nil {
		c.exitCallback()
	}
}
