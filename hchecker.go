package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

const VERSION = "0.1.6"

var (
	myId            string
	cache           *Cache
	runningCheckers = 0
)

func addCheck(line string) {
	check, err := NewCheck(line)
	if err != nil {
		log.Printf("Warning: got invalid data on the \"dead\" "+
			"channel (\"%s\")\n", line)
		return
	}
	if check.BackendGroupLength <= 1 {
		// Add the check only if the frontend is scaled to several
		// backends (backend is part of a group)
		return
	}
	if cache.LockBackend(check) == false {
		return
	}
	// Set all the callbacks for the check. They will be called during
	// the PingUrl at different steps
	check.SetDeadCallback(func() {
		cache.MarkBackendDead(check)
	})
	check.SetAliveCallback(func() {
		cache.MarkBackendAlive(check)
	})
	check.SetCheckIfBreakCallback(func() bool {
		return cache.IsUnlockedBackend(check)
	})
	check.SetExitCallback(func() {
		runningCheckers -= 1
		cache.UnlockBackend(check)
	})
	// Check the URL at a regular interval
	go check.PingUrl()
	runningCheckers += 1
	log.Printf("Added check for: %s\n", check.BackendUrl)
}

/*
 * Prints some stats on runtime
 */
func printStats(cache *Cache) {
	const step = 10 // 10 seconds
	count := 0
	for {
		cache.PingAlive()
		time.Sleep(time.Duration(step) * time.Second)
		count += step
		if count >= 60 {
			// Every minute
			count = 0
			log.Printf("%d backend URLs are being tested\n", runningCheckers)
		}
	}
}

/*
 * Enables CPU profile
 */
func enableCPUProfile() {
	cwd, _ := os.Getwd()
	log.Printf("CPU profile will be written to \"%s/%s\"", cwd, "hchecker.prof")
	f, err := os.Create("hchecker.prof")
	if err != nil {
		log.Fatal("Cannot enable CPU profile:", err)
	}
	pprof.StartCPUProfile(f)
}

/*
 * Listens to signals
 */
func handleSignals() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		switch <-c {
		case syscall.SIGINT:
			pprof.StopCPUProfile()
			os.Exit(0)
		}
	}()
}

func parseFlags(cpuProfile *bool) {
	parseDuration := func(v *time.Duration, n string, def int, help string) {
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
	flag.BoolVar(cpuProfile, "cpuprofile", false,
		"Write CPU profile to \"hchecker.prof\" (current directory)")
	flag.Parse()
}

func main() {
	var (
		err        error
		hostname   string
		cpuProfile bool
	)
	for _, arg := range os.Args {
		if !(arg == "-v" || arg == "--version" || arg == "-version") {
			continue
		}
		fmt.Println("hchecker version", VERSION)
		os.Exit(0)
	}
	parseFlags(&cpuProfile)
	// Force 1 CPU to reduce parallelism. If you want to use more CPUs, prefer
	// spawning several processes instead.
	runtime.GOMAXPROCS(1)
	hostname, _ = os.Hostname()
	myId = fmt.Sprintf("%s#%d", hostname, os.Getpid())
	// Prefix each line of log
	log.SetPrefix(myId + " ")
	if cpuProfile == true {
		enableCPUProfile()
	}
	handleSignals()
	cache, err = NewCache()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = cache.ListenToChannel("dead", addCheck)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	// This function will block and print the stats every minute
	printStats(cache)
}
