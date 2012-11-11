
package main

import (
        "os"
        "fmt"
        "log"
        "time"
        "strings"
        "strconv"
        "encoding/json"
        "net"
        "net/http"
        "sync"
        "runtime"
        "github.com/garyburd/redigo/redis"
        )

var (
    REDIS_KEY = "hchecker"
    // The HTTP method used for each test
    HTTP_METHOD = "HEAD"
    // Check a backend every 3 seconds
    CHECK_INTERVAL = time.Duration(3) * time.Second
    // Check the backend lock every 5 minutes
    CHECK_LOCK = time.Duration(5) * time.Minute
    // Connection timeout is 3 seconds by default
    CONNECTION_TIMEOUT = time.Duration(3) * time.Second
    // IO timeout applies after the connection
    IO_TIMEOUT = time.Duration(3) * time.Second
    // User-Agent for all tests
    USER_AGENT = "dotCloud-HealthCheck/1.0 go/1.0.3"
    )

var (
    MY_ID string
    httpClient *http.Client
    redisConn redis.Conn
    redisMutex sync.Mutex
    runningCheckers = 0
)

type Check struct {
    frontendKey string
    backendUrl string
    backendId int
    backendLength int
}

/*
 * Maintain a mapping between Frontends and Backends ID
 */
func updateFrontendMapping(check *Check, metaKey string, metaData string) {
    var m map[string][]int
    if len(metaData) == 0 {
        // There is no meta-data yet, let's init a mapping
        m = make(map[string][]int)
        m[check.frontendKey] = []int{check.backendId}
        b, _ := json.Marshal(m)
        redisMutex.Lock()
        redisConn.Do("HSET", REDIS_KEY, metaKey, string(b))
        redisMutex.Unlock()
        return
    }
    // There is meta-data, let's load it first
    json.Unmarshal([]byte(metaData), &m)
    if val, ok := m[check.frontendKey]; ok {
        for _, item := range val {
            if item == check.backendId {
                // The backendId has been already stored, ignoring
                return
            }
        }
        // The frontend already exists, adding the new backendId
        m[check.frontendKey] = append(val, check.backendId)
    } else {
        // The frontend is new
        m[check.frontendKey] = []int{check.backendId}
    }
    // Finally, update the meta-data
    b, _ := json.Marshal(m)
    redisMutex.Lock()
    redisConn.Do("HSET", REDIS_KEY, metaKey, string(b))
    redisMutex.Unlock()
}

/*
 * Lock a backend in Redis by its URL
 */
func lockBackend(check *Check) (bool) {
    metaKey := check.backendUrl + ":" + MY_ID
    redisMutex.Lock()
    redisConn.Send("MULTI")
    redisConn.Send("HSETNX", REDIS_KEY, check.backendUrl, MY_ID)
    redisConn.Send("HGET", REDIS_KEY, metaKey)
    reply, _ := redis.Values(redisConn.Do("EXEC"))
    redisMutex.Unlock()
    locked, _ := redis.Bool(reply[0], nil)
    metaData, _ := redis.String(reply[1], nil)
    if locked == false && len(metaData) == 0 {
        // The backend is being monitored by someone else
        return false
    }
    // Let's update the mapping in case this is a new frontend
    updateFrontendMapping(check, metaKey, metaData)
    return locked
}

func isLockedBackend(check *Check) (bool) {
    redisMutex.Lock()
    reply, _ := redis.Bool(redisConn.Do("HEXISTS", REDIS_KEY,
        check.backendUrl))
    redisMutex.Unlock()
    return reply
}

func unlockBackend(check *Check) {
    redisMutex.Lock()
    redisConn.Do("HDEL", REDIS_KEY, check.backendUrl,
        check.backendUrl + ":" + MY_ID)
    redisMutex.Unlock()
}

func createHttpTransport() (*http.Transport) {
    httpDial := func (proto string, addr string) (net.Conn, error) {
        conn, err := net.DialTimeout(proto, addr, CONNECTION_TIMEOUT)
        if err != nil {
            return nil, err
        }
        conn.SetDeadline(time.Now().Add(IO_TIMEOUT))
        return conn, nil
    }
    return &http.Transport{
        DisableKeepAlives: true,
        Dial: httpDial,
    }
}

func getBackendMetaData(check *Check) map[string][]int {
    var m map[string][]int
    metaKey := check.backendUrl + ":" + MY_ID
    redisMutex.Lock()
    metaData, _ := redis.String(redisConn.Do("HGET", REDIS_KEY, metaKey))
    redisMutex.Unlock()
    json.Unmarshal([]byte(metaData), &m)
    return m
}

func markBackendDead(check *Check) {
    m := getBackendMetaData(check)
    redisMutex.Lock()
    redisConn.Send("MULTI")
    for frontendKey, ids := range m {
        deadKey := "dead:" + frontendKey
        for _, id := range ids {
            redisConn.Send("SADD", deadKey, id)
        }
        // Better way would be to set the same TTL than Hipache. Not critical
        // since we'll clean the backend list
        redisConn.Send("EXPIRE", deadKey, 60)
    }
    redisConn.Do("EXEC")
    redisMutex.Unlock()
}

func markBackendAlive(check *Check) {
    m := getBackendMetaData(check)
    redisMutex.Lock()
    redisConn.Send("MULTI")
    for frontendKey, ids := range m {
        for _, id := range ids {
            redisConn.Send("SREM", "dead:" + frontendKey, id)
        }
    }
    redisConn.Do("EXEC")
    redisMutex.Unlock()
}

func pingUrl(check *Check) {
    // Current status, true for alive, false for dead
    var (
        testError string
        lastMarkDeadCall time.Time
        status = true
        newStatus = true
        i = time.Duration(0)
        )
    for {
        req, _ := http.NewRequest(HTTP_METHOD, check.backendUrl, nil)
        req.Header.Add("User-Agent", USER_AGENT)
        resp, err := httpClient.Do(req)
        if err != nil {
            // TCP error
            newStatus = false
            testError = fmt.Sprintf("TCP error on %s: %#v",
                check.backendUrl, err.Error())
        } else {
            // No TCP error, checking HTTP code
            if resp.StatusCode > 500 && resp.StatusCode < 600 &&
                resp.StatusCode != 503 {
                    newStatus = false
                    testError = fmt.Sprintf("HTTP error on %s: %#v",
                        check.backendUrl, resp.StatusCode)
                }
        }
        // Check if the status changed before updating Redis
        if newStatus != status {
            if newStatus == true {
                log.Printf("%s is back online\n", check.backendUrl)
                markBackendAlive(check)
                lastMarkDeadCall = time.Time{}
            } else {
                log.Println(testError)
                markBackendDead(check)
                lastMarkDeadCall = time.Now()
            }
        } else if newStatus == false {
            // Backend is still dead. Mark it as dead every 30 seconds to keep
            // it dead despite the Redis TTL
            if lastMarkDeadCall.IsZero() == false &&
                time.Since(lastMarkDeadCall) >=
                (time.Duration(30) * time.Second) {
                    markBackendDead(check)
                    lastMarkDeadCall = time.Now()
                }
        }
        status = newStatus
        time.Sleep(CHECK_INTERVAL)
        i += CHECK_INTERVAL
        // At longer interval, we check if still have the lock on the backend
        if i >= CHECK_LOCK {
            if isLockedBackend(check) == false {
                // We lost the lock, terminate the goroutine
                runningCheckers -= 1
                break
            }
            i = time.Duration(0)
        }
    }
}

func addCheck(check *Check) {
    r := lockBackend(check)
    if r == false {
        // Backend already locked
        return
    }
    go pingUrl(check)
    runningCheckers += 1
    log.Printf("Added check for: %s\n", check.backendUrl)
}

func listenToRedis() {
    // Listening on the "dead" channel to get dead notifications by Hipache
    // Format received on the channel is:
    // -> frontend_key;backend_url;backend_id;number_of_backends
    // Example: "localhost;http://localhost:4242;0;1"
    // Create a new connection to the Redis for the SUBSCRIBE
    conn, _ := redis.Dial("tcp", ":6379")
    defer conn.Close()
    psc := redis.PubSubConn{conn}
    psc.Subscribe("dead")
    for {
        switch v := psc.Receive().(type) {
            case redis.Message:
                line := string(v.Data)
                parts := strings.Split(strings.TrimSpace(line), ";")
                if len(parts) != 4 {
                    log.Printf("Warning: got invalid data on the \"dead\" " +
                        "channel (\"%s\")\n", line)
                    break
                }
                check := &Check{}
                check.frontendKey = parts[0]
                check.backendUrl = parts[1]
                check.backendId, _ = strconv.Atoi(parts[2])
                check.backendLength, _ = strconv.Atoi(parts[3])
                addCheck(check)
            case error:
                log.Printf("Error: cannot subscribe on the \"dead\" " +
                    "channel: %#v\n", v)
                return
        }
    }
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
    MY_ID = fmt.Sprintf("%s#%d", hostname, os.Getpid())
    // Prefix each line of log
    log.SetPrefix(MY_ID + " ")
    redisConn, err = redis.Dial("tcp", ":6379")
    if err != nil {
        log.Printf("Cannot connect to Redis (%s)\n", err.Error())
        os.Exit(1)
    }
    httpClient = &http.Client{Transport: createHttpTransport()}
    defer redisConn.Close()
    printStats()
    listenToRedis()
}
