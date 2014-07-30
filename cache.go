package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

const (
	REDIS_PREFFIX      = "hchecker"
	REDIS_ADDRESS      = "localhost:6379"
	REDIS_PASSWORD     = ""
	REDIS_IDLE_TIMEOUT = 120
	REDIS_MAX_IDLE     = 3
)

var (
	redisAddress     string
	redisPassword    string
	redisSuffix      string
	redisMaxIdle     int
	redisIdleTimeout int
)

type Cache struct {
	pool     *redis.Pool
	redisKey string
	// Maintain a mapping between a backends and several frontend
	// -> map[BACKEND_URL][FRONTEND_NAME] = BACKEND_ID
	backendsMapping map[string]map[string]int
	// Channel used to notify goroutine when a frontend has been added to the
	// backendsMapping
	channelMapping map[string]chan int
}

func NewCache() (*Cache, error) {
	var redisKey string
	if redisSuffix != "" {
		redisKey = REDIS_PREFFIX + "_" + redisSuffix
	} else {
		redisKey = REDIS_PREFFIX
	}
	cache := &Cache{
		redisKey:        redisKey,
		backendsMapping: make(map[string]map[string]int),
		channelMapping:  make(map[string]chan int),
	}
	cache.pool = &redis.Pool{
		MaxIdle:     redisMaxIdle,
		IdleTimeout: time.Duration(redisIdleTimeout) * time.Second,
		Dial:        cache.getConn,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	// We're starting, let's clear any previous meta-data
	// WARNING: This can be a problem if there are several processes sharing
	// the same redis on the same machine - without specifying redis_suffix option.
	// If one of them is restarted, it'll clear the meta-data of everyone...
	conn := cache.pool.Get()
	defer conn.Close()
	conn.Send("DEL", cache.redisKey)
	return cache, nil
}

func (c *Cache) getConn() (redis.Conn, error) {
	conn, err := redis.Dial("tcp", redisAddress)
	if err != nil {
		return nil, err
	}
	if redisPassword != "" {
		if _, err := conn.Do("AUTH", redisPassword); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, err
}

/*
 * Maintain a mapping between Frontends and Backends ID
 */
func (c *Cache) updateFrontendMapping(check *Check) {
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		m = make(map[string]int)
	}
	m[check.FrontendKey] = check.BackendId
	c.backendsMapping[check.BackendUrl] = m
	// Notify the goroutine that we added a frontend
	ch, exists := c.channelMapping[check.BackendUrl]
	if exists {
		// Non-blocking send
		select {
		case ch <- 1:
		default:
		}
	}
}

/*
 * Lock a backend in Redis by its URL
 */
func (c *Cache) LockBackend(check *Check) (bool, chan int) {
	// The syncKey makes sure an entire backend mapping is keep in the same
	// process (we never update a backend mapping from 2 different processes)
	syncKey := check.BackendUrl + ";" + myId
	// Lock the backend with a temporary value, we'll update this with the
	// goroutine signature later
	var locked bool
	var isMine bool
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("MULTI")
	conn.Send("HSETNX", c.redisKey, check.BackendUrl, 1)
	conn.Send("HEXISTS", c.redisKey, syncKey)
	resp, _ := redis.Values(conn.Do("EXEC"))
	redis.Scan(resp, &locked, &isMine)
	if locked == false && isMine == false {
		// The backend is being monitored by someone else
		return false, nil
	}
	if locked == false {
		c.updateFrontendMapping(check)
		return false, nil
	}
	// we got the lock, let's create a unique sig for the goroutine
	t := time.Now()
	// This one is done in the lock, this will garanty that no routine
	// will get the same sig
	sig := fmt.Sprintf("%s;%d.%d", myId, t.Unix(), t.Nanosecond())
	conn.Send("HSET", c.redisKey, check.BackendUrl, sig)
	conn.Send("HSET", c.redisKey, syncKey, 1)
	conn.Flush()
	check.routineSig = sig
	// Create the channel
	ch := make(chan int, 1)
	c.channelMapping[check.BackendUrl] = ch
	c.updateFrontendMapping(check)
	return true, ch
}

func (c *Cache) IsUnlockedBackend(check *Check) bool {
	// On top of checking the lock, we compare the lock content to make sure
	// we still own the lock
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("HGET", c.redisKey, check.BackendUrl)
	conn.Flush()
	resp, _ := redis.String(conn.Receive())
	return (resp != check.routineSig)
}

func (c *Cache) UnlockBackend(check *Check) {
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("HDEL", c.redisKey, check.BackendUrl, check.BackendUrl+";"+myId)
	conn.Flush()
	delete(c.backendsMapping, check.BackendUrl)
	delete(c.channelMapping, check.BackendUrl)
}

/*
 * Before changing the state (dead or alive) in the Redis, we make sure
 * the backend is still both in memory and in Redis so we'll avoid wrong
 * updates.
 */
func (c *Cache) checkBackendMapping(check *Check, frontendKey string,
	backendId int, mapping *map[string]int) bool {
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("LINDEX", "frontend:"+frontendKey, backendId+1)
	conn.Flush()
	resp, _ := redis.String(conn.Receive())
	if resp == check.BackendUrl {
		return true
	}
	log.Println(check.BackendUrl, "Mapping changed for", frontendKey)
	delete(*mapping, frontendKey)
	return false
}

/*
 * Flag the backend dead in Redis
 * Returns false if no update has been performed (backend unlock)
 */
func (c *Cache) MarkBackendDead(check *Check) bool {
	conn := c.pool.Get()
	defer conn.Close()
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		c.UnlockBackend(check)
		return false
	}
	conn.Send("MULTI")
	for frontendKey, id := range m {
		if r := c.checkBackendMapping(check, frontendKey, id, &m); r == false {
			continue
		}
		deadKey := "dead:" + frontendKey
		conn.Send("SADD", deadKey, id)
		// Better way would be to set the same TTL than Hipache. Not
		// critical since we'll clean the backend list
		conn.Send("EXPIRE", deadKey, 60)
	}
	conn.Do("EXEC")
	if len(m) == 0 {
		// checkBackenMapping() removed all frontend mapping, no need to check
		// this backend anymore...
		c.UnlockBackend(check)
		return false
	}
	return true
}

/*
 * Flag the backend live in Redis
 * Returns false if no update has been performed (backend unlock)
 */
func (c *Cache) MarkBackendAlive(check *Check) bool {
	conn := c.pool.Get()
	defer conn.Close()
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		c.UnlockBackend(check)
		return false
	}
	conn.Send("MULTI")
	for frontendKey, id := range m {
		if r := c.checkBackendMapping(check, frontendKey, id, &m); r == false {
			continue
		}
		conn.Send("SREM", "dead:"+frontendKey, id)
	}
	conn.Do("EXEC")
	if len(m) == 0 {
		c.UnlockBackend(check)
		return false
	}
	return true
}

func (c *Cache) ListenToChannel(channel string, callback func(line string)) error {
	// Listening on the "dead" channel to get dead notifications by Hipache
	// Format received on the channel is:
	// -> frontend_key;backend_url;backend_id;number_of_backends
	// Example: "localhost;http://localhost:4242;0;1"
	go func() {
		for {
			err := c.connectAndListen(channel, callback)
			if err != nil {
				log.Printf("Error subscribing channel %q: %s. Reconnecting...", channel, err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()
	return nil
}

func (c *Cache) connectAndListen(channel string, callback func(line string)) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}
	psc := redis.PubSubConn{conn}
	defer psc.Close()
	psc.Subscribe(channel)
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			callback(string(v.Data[:]))
		case error:
			return v
		}
	}
}

func (c *Cache) PingAlive() {
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("SET", "hchecker_ping", time.Now().Unix())
	conn.Flush()
}
