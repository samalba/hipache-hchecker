
package main

import (
    "fmt"
    "sync"
    "errors"
    "encoding/json"
    "github.com/garyburd/redigo/redis"
)

const (
    REDIS_KEY = "hchecker"
)

type Cache struct {
    redisConn redis.Conn
    redisMutex sync.Mutex
}

func NewCache() (*Cache, error) {
    redisConn, err := redis.Dial("tcp", ":6379")
    if err != nil {
        return nil, errors.New(fmt.Sprintf("Cannot connect to Redis (%s)",
            err.Error()))
    }
    return &Cache{redisConn: redisConn}, nil
}

/*
 * Maintain a mapping between Frontends and Backends ID
 */
func (c *Cache) updateFrontendMapping(check *Check, metaKey string, metaData string) {
    var m map[string][]int
    if len(metaData) == 0 {
        // There is no meta-data yet, let's init a mapping
        m = make(map[string][]int)
        m[check.FrontendKey] = []int{check.BackendId}
        b, _ := json.Marshal(m)
        c.redisMutex.Lock()
        c.redisConn.Do("HSET", REDIS_KEY, metaKey, string(b))
        c.redisMutex.Unlock()
        return
    }
    // There is meta-data, let's load it first
    json.Unmarshal([]byte(metaData), &m)
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
    // Finally, update the meta-data
    b, _ := json.Marshal(m)
    c.redisMutex.Lock()
    c.redisConn.Do("HSET", REDIS_KEY, metaKey, string(b))
    c.redisMutex.Unlock()
}

/*
 * Lock a backend in Redis by its URL
 */
func (c *Cache) LockBackend(check *Check) (bool) {
    metaKey := check.BackendUrl + ":" + myId
    c.redisMutex.Lock()
    c.redisConn.Send("MULTI")
    // Lock the backend with a temporary value, we'll update this with the
    // goroutine signature later
    c.redisConn.Send("HSETNX", REDIS_KEY, check.BackendUrl, 1)
    c.redisConn.Send("HGET", REDIS_KEY, metaKey)
    reply, _ := redis.Values(c.redisConn.Do("EXEC"))
    c.redisMutex.Unlock()
    locked, _ := redis.Bool(reply[0], nil)
    metaData, _ := redis.String(reply[1], nil)
    if locked == false && len(metaData) == 0 {
        // The backend is being monitored by someone else
        return false
    }
    // Let's update the mapping in case this is a new frontend
    c.updateFrontendMapping(check, metaKey, metaData)
    return locked
}

func (c *Cache) IsUnlockedBackend(check *Check) (bool) {
    c.redisMutex.Lock()
    // FIXME: Get the content of the lock and compare with the goroutine sig
    reply, _ := redis.Bool(c.redisConn.Do("HEXISTS", REDIS_KEY,
        check.BackendUrl))
    c.redisMutex.Unlock()
    return !reply
}

func (c *Cache) UnlockBackend(check *Check) {
    c.redisMutex.Lock()
    c.redisConn.Do("HDEL", REDIS_KEY, check.BackendUrl,
        check.BackendUrl + ":" + myId)
    c.redisMutex.Unlock()
}

func (c *Cache) getBackendMetaData(check *Check) map[string][]int {
    var m map[string][]int
    metaKey := check.BackendUrl + ":" + myId
    c.redisMutex.Lock()
    metaData, _ := redis.String(c.redisConn.Do("HGET", REDIS_KEY, metaKey))
    c.redisMutex.Unlock()
    json.Unmarshal([]byte(metaData), &m)
    return m
}

func (c *Cache) MarkBackendDead(check *Check) {
    m := c.getBackendMetaData(check)
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
    m := c.getBackendMetaData(check)
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
    conn, _ := redis.Dial("tcp", ":6379")
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
