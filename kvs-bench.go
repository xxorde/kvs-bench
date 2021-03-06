package main

import (
	"flag"
	"math/rand"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/xxorde/kvs"
)

func main() {
	var nKeys int64
	var nRndWrites int64
	var nRndReads int64
	var seed int64
	var dumpfile string
	var backup bool
	const line = "==================================================================="
	flag.Int64Var(&nKeys, "keys", 1000000, "Number of key / value pairs")
	flag.Int64Var(&nRndWrites, "writes", 1000000, "Number of random writes")
	flag.Int64Var(&nRndReads, "reads", 1000000, "Number of random reads")
	flag.Int64Var(&seed, "seed", 1337, "seed for rand")
	flag.BoolVar(&backup, "backup", true, "should backup be performed?")
	flag.StringVar(&dumpfile, "of", "kvs.dump", "output file for the dump")
	flag.Parse()

	rand.Seed(int64(seed))

	// initialize kvs
	//store := kvs.Kvs{M: make(map[string]string)}
	store := kvs.NewKvs()

	// create n key / values
	start := time.Now()
	log.Info(line)
	log.Infof("Create %d key / value pairs", nKeys)
	for i := int64(0); i < nKeys; i++ {
		go store.Put("key"+strconv.FormatInt(i, 10), "value"+strconv.FormatInt(i, 10))
		if i < i-1 {
			log.Info("Overflow: ", i)
		}
	}
	log.Info("Time: ", time.Since(start),
		"time per key: ", time.Since(start).Nanoseconds()/int64(nKeys), "ns")

	// write random value
	start = time.Now()
	log.Info(line)
	log.Infof("Do %d random writes", nRndWrites)
	for i := int64(0); i < nRndWrites; i++ {
		store.Put("key"+strconv.FormatInt(rand.Int63n(nKeys), 10), "random write"+strconv.FormatInt(i, 10))
	}
	log.Info("Time: ", time.Since(start),
		"time per write: ", time.Since(start).Nanoseconds()/int64(nRndWrites), "ns")

	// read random value
	start = time.Now()
	log.Info(line)
	log.Infof("Do %d random reads", nRndReads)
	tmp := ""
	for i := int64(0); i < nRndReads; i++ {
		tmp = store.Get("key" + strconv.FormatInt(rand.Int63n(nKeys), 10))
	}
	// use it or get en error :)
	//tmp = tmp
	log.Info("Last read value:", tmp)
	log.Info("Time: ", time.Since(start),
		"time per read: ", time.Since(start).Nanoseconds()/int64(nRndReads), "ns")

	// check random tuple
	start = time.Now()
	log.Info(line)
	log.Infof("Do %d random tuple checks", nRndReads)
	for i := int64(0); i < nRndReads; i++ {
		store.Exists("key" + strconv.FormatInt(rand.Int63n(nKeys), 10))
	}
	log.Info("Time: ", time.Since(start),
		"time per check: ", time.Since(start).Nanoseconds()/int64(nRndReads), "ns")

	// check random tuple
	start = time.Now()
	log.Info(line)
	log.Infof("Do %d random tuple checks on NONEXISTING tuples", nRndReads)
	for i := int64(0); i < nRndReads; i++ {
		store.Exists("NOTHERE" + strconv.FormatInt(rand.Int63n(nKeys), 10))
	}
	log.Info("Time: ", time.Since(start),
		"time per check: ", time.Since(start).Nanoseconds()/int64(nRndReads), "ns")

	// some info
	log.Info(line)
	log.Info("Size of kvs is: ", store.Len())

	if backup {
		// write to file
		start = time.Now()
		log.Info(line)
		log.Info("Write data to file")
		f, err := os.Create(dumpfile)
		if err != nil {
			panic("cant open file")
		}
		store.DumpYaml(f)
		f.Sync()
		f.Close()
		log.Info("Time: ", time.Since(start),
			"time per dump: ", time.Since(start).Nanoseconds()/int64(nKeys), "ns")

		/*// compress and write to file
		start = time.Now()
		log.Info(line)
		log.Info("Compress write data to file")
		store.BackupBinGz(dumpfile+".gz")
		log.Info("Time: ", time.Since(start),
		"time per dump: ", time.Since(start).Nanoseconds()/int64(nKeys), "ns")*/
	}

	log.Info(line)
}
