package main

import (
	"time"
	"flag"
	"fmt"
	"math/rand"
	"github.com/xxorde/kvs"
)

func main() {
	var nKeys int64
	var nRndWrites int64
	var nRndReads int64
	var seed int64
	var dumpfile string
	line := "==================================================================="
	flag.Int64Var(&nKeys, "keys", 1000000, "Number of key / value pairs")
	flag.Int64Var(&nRndWrites, "writes", 1000000, "Number of random writes")
	flag.Int64Var(&nRndReads, "reads", 1000000, "Number of random reads")
	flag.Int64Var(&seed, "seed", 1337, "seed for rand")
	flag.StringVar(&dumpfile, "of", "kvs.dump", "output file for the dump")
	flag.Parse()

	rand.Seed(int64(seed))

	// initialize kvs
	store := kvs.Kvs{M: make(map[string]string)}

	// create n key / values
	start := time.Now()
	fmt.Println(line)
	fmt.Printf("Create %d key / value pairs\n", nKeys)
	for i:=int64(0); i < nKeys; i++ {
		//m["key"+string(i)]="value"+string(i)
		store.Put("key"+string(i), "value"+string(i))
	}
	fmt.Println("Time: ", time.Since(start),
		"time per key: ", time.Since(start).Nanoseconds()/int64(nKeys), "ns")

	// write random value
	start = time.Now()
	fmt.Println(line)
	fmt.Printf("Do %d random writes\n", nRndWrites)
	for i:=int64(0); i < nRndWrites; i++ {
		//m["key"+string(rand.Intn(nKeys))]="random write"+string(i)
		store.Put("key"+string(rand.Int63n(nKeys)), "random write"+string(i))
	}
	fmt.Println("Time: ", time.Since(start),
	"time per write: ", time.Since(start).Nanoseconds()/int64(nRndWrites), "ns")

	// read random value
	start = time.Now()
	fmt.Println(line)
	fmt.Printf("Do %d random reads\n", nRndReads)
	tmp := ""
	for i:=int64(0); i < nRndReads; i++ {
		//tmp = m["key"+string(rand.Intn(nKeys))]
		tmp = store.Get("key"+string(rand.Int63n(nKeys)))
	}
	// use it or get en error :)
	tmp = tmp
	fmt.Println("Time: ", time.Since(start),
	"time per read: ", time.Since(start).Nanoseconds()/int64(nRndReads), "ns")

	// write to file
	start = time.Now()
	fmt.Println(line)
	fmt.Println("Write data to file")
	store.Dump(dumpfile)
	fmt.Println("Time: ", time.Since(start),
	"time per dump: ", time.Since(start).Nanoseconds()/int64(nKeys), "ns")
	fmt.Println(line)
}
