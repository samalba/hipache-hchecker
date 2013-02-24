package main

import (
	"errors"
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"time"
)

const (
	REDIS_KEY     = "hchecker"
	REDIS_ADDRESS = "localhost:6379"
)

var (
	redisAddress string
)

type Cache struct {
	redisConn *redis.Client
	redisSub  *redis.Subscription
	// Maintain a mapping between a backends and several frontend
	// -> map[BACKEND_URL][FRONTEND_NAME] = BACKEND_ID
	backendsMapping map[string]map[string]int
	// Channel used to notify goroutine when a frontend has been added to the
	// backendsMapping
	channelMapping map[string]chan int
}

func NewCache() (*Cache, error) {
	conf := redis.DefaultConfig()
	conf.Address = redisAddress
	redisConn := redis.NewClient(conf)
	cache := &Cache{
		redisConn:       redisConn,
		backendsMapping: make(map[string]map[string]int),
		channelMapping:  make(map[string]chan int),
	}
	// We're starting, let's clear any previous meta-data
	// WARNING: This can be a problem if there are several processes sharing
	// the same redis on the same machine. If one of them is restarted, it'll
	// clear the meta-data of everyone...
	redisConn.Del(REDIS_KEY)
	return cache, nil
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
	resp := c.redisConn.Transaction(func(mc *redis.MultiCall) {
		mc.Hsetnx(REDIS_KEY, check.BackendUrl, 1)
		mc.Hexists(REDIS_KEY, syncKey)
	})
	locked, _ := resp.Elems[0].Bool()
	isMine, _ := resp.Elems[1].Bool()
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
	c.redisConn.Hset(REDIS_KEY, check.BackendUrl, sig)
	c.redisConn.Hset(REDIS_KEY, syncKey, 1)
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
	resp, _ := c.redisConn.Hget(REDIS_KEY, check.BackendUrl).Str()
	return (resp != check.routineSig)
}

func (c *Cache) UnlockBackend(check *Check) {
	c.redisConn.Hdel(REDIS_KEY, check.BackendUrl,
		check.BackendUrl+":"+myId)
	delete(c.backendsMapping, check.BackendUrl)
	delete(c.channelMapping, check.BackendUrl)
}

func (c *Cache) checkBackendMapping(check *Check, frontendKey string,
	backendId int, mapping *map[string]int) bool {
	// Before changing the state (dead or alive) in the Redis, we make sure
	// the backend is still both in memory and in Redis so we'll avoid wrong
	// updates.
	resp, _ := c.redisConn.Lindex("frontend:"+frontendKey, backendId+1).Str()
	if resp == check.BackendUrl {
		return true
	}
	log.Println(check.BackendUrl, "Mapping changed for", frontendKey)
	delete(*mapping, frontendKey)
	return false
}

func (c *Cache) MarkBackendDead(check *Check) {
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		c.UnlockBackend(check)
		return
	}
	c.redisConn.Transaction(func(mc *redis.MultiCall) {
		for frontendKey, id := range m {
			if r := c.checkBackendMapping(check, frontendKey, id, &m); r == false {
				continue
			}
			deadKey := "dead:" + frontendKey
			mc.Sadd(deadKey, id)
			// Better way would be to set the same TTL than Hipache. Not
			// critical since we'll clean the backend list
			mc.Expire(deadKey, 60)
		}
	})
	if len(m) == 0 {
		// checkBackenMapping() removed all frontend mapping, no need to check
		// this backend anymore...
		c.UnlockBackend(check)
	}
}

func (c *Cache) MarkBackendAlive(check *Check) {
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		c.UnlockBackend(check)
		return
	}
	c.redisConn.Transaction(func(mc *redis.MultiCall) {
		for frontendKey, id := range m {
			if r := c.checkBackendMapping(check, frontendKey, id, &m); r == false {
				continue
			}
			mc.Srem("dead:"+frontendKey, id)
		}
	})
	if len(m) == 0 {
		c.UnlockBackend(check)
	}
}

func (c *Cache) ListenToChannel(channel string, callback func(line string)) error {
	// Listening on the "dead" channel to get dead notifications by Hipache
	// Format received on the channel is:
	// -> frontend_key;backend_url;backend_id;number_of_backends
	// Example: "localhost;http://localhost:4242;0;1"
	msgHandler := func(msg *redis.Message) {
		switch msg.Type {
		case redis.MessageMessage:
			callback(msg.Payload)
		}
	}
	sub, err := c.redisConn.Subscription(msgHandler)
	if err != nil {
		return errors.New(fmt.Sprintf("Error: cannot subscribe to "+
			"the \"dead\" channel: %#v", err))
	}
	sub.Subscribe(channel)
	c.redisSub = sub
	return nil
}

func (c *Cache) PingAlive() {
	c.redisConn.Set("hchecker_ping", time.Now().Unix())
}
