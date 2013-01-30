
package main

import (
    "fmt"
    "time"
    "sync"
    "errors"
    "github.com/garyburd/redigo/redis"
)

const (
    REDIS_KEY = "hchecker"
    REDIS_ADDRESS = ":6379"
)

var (
    redisAddress string
)

type Cache struct {
    redisConn redis.Conn
    redisMutex sync.Mutex
    // Maintain a mapping between a backends and several frontend
    // -> map[BACKEND_URL][FRONTEND_NAME][BACKEND_ID]
    backendsMapping map[string]map[string][]int
}

func NewCache() (*Cache, error) {
    redisConn, err := redis.Dial("tcp", redisAddress)
    if err != nil {
        return nil, errors.New(fmt.Sprintf("Cannot connect to Redis (%s)",
            err.Error()))
    }
    cache := &Cache{
        redisConn: redisConn,
        backendsMapping: make(map[string]map[string][]int)}
    // We're starting, let's clear any previous meta-data
    // WARNING: This can be a problem if there are several processes sharing
    // the same redis on the same machine. If one of them is restarted, it'll
    // clear the meta-data of everyone...
    redisConn.Do("DEL", REDIS_KEY)
    return cache, nil
}

/*
 * Maintain a mapping between Frontends and Backends ID
 */
func (c *Cache) updateFrontendMapping(check *Check) {
    m, exists := c.backendsMapping[check.BackendUrl]
    if !exists {
        m = make(map[string][]int)
    }
    if val, ok := m[check.FrontendKey]; ok {
        for _, item := range val {
            if item == check.BackendId {
                // The backendId has been already stored, ignoring
                return
            }
        }
        // The frontend already exists, adding the new backendId
        m[check.FrontendKey] = append(val, check.BackendId)
    } else {
        // The frontend is new
        m[check.FrontendKey] = []int{check.BackendId}
    }
    c.backendsMapping[check.BackendUrl] = m
}

/*
 * Lock a backend in Redis by its URL
 */
func (c *Cache) LockBackend(check *Check) bool {
    // The syncKey makes sure an entire backend mapping is keep in the same
    // process (we never update a backend mapping from 2 different processes)
    syncKey := check.BackendUrl + ";" + myId
    c.redisMutex.Lock()
    c.redisConn.Send("MULTI")
    // Lock the backend with a temporary value, we'll update this with the
    // goroutine signature later
    c.redisConn.Send("HSETNX", REDIS_KEY, check.BackendUrl, 1)
    c.redisConn.Send("HEXISTS", REDIS_KEY, syncKey)
    reply, _ := redis.Values(c.redisConn.Do("EXEC"))
    c.redisMutex.Unlock()
    locked, _ := redis.Bool(reply[0], nil)
    isMine, _ := redis.Bool(reply[1], nil)
    if locked == false && isMine == false {
        // The backend is being monitored by someone else
        return false
    }
    if locked == true {
        // we got the lock, let's create a unique sig for the goroutine
        c.redisMutex.Lock()
        t := time.Now()
        // This one is done in the lock, this will garanty that no routine
        // will get the same sig
        sig := fmt.Sprintf("%s;%d.%d", myId, t.Unix(), t.Nanosecond())
        c.redisConn.Do("HSET", REDIS_KEY, check.BackendUrl, sig)
        c.redisConn.Do("HSET", REDIS_KEY, syncKey, 1)
        check.routineSig = sig
        c.redisMutex.Unlock()
    }
    // Let's update the mapping in case this is a new frontend
    c.updateFrontendMapping(check)
    return locked
}

func (c *Cache) IsUnlockedBackend(check *Check) bool {
    c.redisMutex.Lock()
    // On top of checking the lock, we compare the lock content to make sure
    // we still own the lock
    reply, _ := redis.String(c.redisConn.Do("HGET", REDIS_KEY, check.BackendUrl))
    c.redisMutex.Unlock()
    return (reply != check.routineSig)
}

func (c *Cache) UnlockBackend(check *Check) {
    c.redisMutex.Lock()
    c.redisConn.Do("HDEL", REDIS_KEY, check.BackendUrl,
        check.BackendUrl + ":" + myId)
    c.redisMutex.Unlock()
    delete(c.backendsMapping, check.BackendUrl)
}

func (c *Cache) MarkBackendDead(check *Check) {
    m, exists := c.backendsMapping[check.BackendUrl]
    if !exists {
        return
    }
    c.redisMutex.Lock()
    c.redisConn.Send("MULTI")
    for frontendKey, ids := range m {
        deadKey := "dead:" + frontendKey
        for _, id := range ids {
            c.redisConn.Send("SADD", deadKey, id)
        }
        // Better way would be to set the same TTL than Hipache. Not critical
        // since we'll clean the backend list
        c.redisConn.Send("EXPIRE", deadKey, 60)
    }
    c.redisConn.Do("EXEC")
    c.redisMutex.Unlock()
}

func (c *Cache) MarkBackendAlive(check *Check) {
    m, exists := c.backendsMapping[check.BackendUrl]
    if !exists {
        return
    }
    c.redisMutex.Lock()
    c.redisConn.Send("MULTI")
    for frontendKey, ids := range m {
        for _, id := range ids {
            c.redisConn.Send("SREM", "dead:" + frontendKey, id)
        }
    }
    c.redisConn.Do("EXEC")
    c.redisMutex.Unlock()
}

func (c *Cache) ListenToChannel(channel string, callback func (line string)) error {
    // Listening on the "dead" channel to get dead notifications by Hipache
    // Format received on the channel is:
    // -> frontend_key;backend_url;backend_id;number_of_backends
    // Example: "localhost;http://localhost:4242;0;1"
    // Create a new connection to the Redis for the SUBSCRIBE
    conn, _ := redis.Dial("tcp", redisAddress)
    defer conn.Close()
    psc := redis.PubSubConn{conn}
    psc.Subscribe(channel)
    for {
        switch v := psc.Receive().(type) {
            case redis.Message:
                callback(string(v.Data))
            case error:
                return errors.New(fmt.Sprintf("Error: cannot subscribe on " +
                    "the \"dead\" channel: %#v", v))
        }
    }
    return nil
}
