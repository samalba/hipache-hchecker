package main

import (
	"errors"
	"fmt"
	"github.com/fzzy/radix/redis"
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
	// -> map[BACKEND_URL][FRONTEND_NAME][BACKEND_ID]
	backendsMapping map[string]map[string][]int
}

func NewCache() (*Cache, error) {
	conf := redis.DefaultConfig()
	conf.Address = redisAddress
	redisConn := redis.NewClient(conf)
	cache := &Cache{
		redisConn:       redisConn,
		backendsMapping: make(map[string]map[string][]int)}
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
		return false
	}
	if locked == true {
		// we got the lock, let's create a unique sig for the goroutine
		t := time.Now()
		// This one is done in the lock, this will garanty that no routine
		// will get the same sig
		sig := fmt.Sprintf("%s;%d.%d", myId, t.Unix(), t.Nanosecond())
		c.redisConn.Hset(REDIS_KEY, check.BackendUrl, sig)
		c.redisConn.Hset(REDIS_KEY, syncKey, 1)
		check.routineSig = sig
	}
	// Let's update the mapping in case this is a new frontend
	c.updateFrontendMapping(check)
	return locked
}

func (c *Cache) IsUnlockedBackend(check *Check) bool {
	// On top of checking the lock, we compare the lock content to make sure
	// we still own the lock
	resp, _ := c.redisConn.Hget(REDIS_KEY, check.BackendUrl).Str()
	return (resp != check.routineSig)
}

func (c *Cache) UnlockBackend(check *Check) {
	c.redisConn.Hdel(REDIS_KEY, check.BackendUrl,
		check.BackendUrl + ":" + myId)
	delete(c.backendsMapping, check.BackendUrl)
}

func (c *Cache) MarkBackendDead(check *Check) {
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		return
	}
	c.redisConn.Transaction(func(mc *redis.MultiCall) {
		for frontendKey, ids := range m {
			deadKey := "dead:" + frontendKey
			for _, id := range ids {
				mc.Sadd(deadKey, id)
			}
			// Better way would be to set the same TTL than Hipache. Not critical
			// since we'll clean the backend list
			mc.Expire(deadKey, 60)
		}
	})
}

func (c *Cache) MarkBackendAlive(check *Check) {
	m, exists := c.backendsMapping[check.BackendUrl]
	if !exists {
		return
	}
	c.redisConn.Transaction(func(mc *redis.MultiCall) {
		for frontendKey, ids := range m {
			for _, id := range ids {
				mc.Srem("dead:"+frontendKey, id)
			}
		}
	})
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
