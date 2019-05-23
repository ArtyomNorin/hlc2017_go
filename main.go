package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
)

func main() {
	fmt.Println(os.Getpid())

	database, err := InitDatabase("/tmp/hlc/data", "/tmp/data/options.txt")
	//database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/full/data", "/home/artyomnorin/Projects/hlc2017_go/data/full/options.txt")

	if err != nil {
		log.Fatalln(err)
	}

	database.SortIndexes()

	debug.FreeOSMemory()

	database.PrintStats()

	PrintMemStats()

	NewServer(database).Run(80)
}

func PrintMemStats() {
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)

	fmt.Println(fmt.Sprintf("Total alloc: %d", memstats.TotalAlloc))
	fmt.Println(fmt.Sprintf("Count GC: %d", memstats.NumGC))
}
