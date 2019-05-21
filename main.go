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

	//database, err := InitDatabase("/tmp/hlc/data", "/tmp/data/options.txt")
	database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/train/data", "/home/artyomnorin/Projects/hlc2017_go/data/train/options.txt")

	if err != nil {
		log.Fatalln(err)
	}

	database.SortIndexes()

	debug.FreeOSMemory()

	database.PrintStats()

	PrintMemStats()

	NewServer(database).Run(8080)
}

func PrintMemStats() {
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)

	fmt.Println(fmt.Sprintf("Total alloc: %d", memstats.TotalAlloc))
	fmt.Println(fmt.Sprintf("Count GC: %d", memstats.NumGC))
}
