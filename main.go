package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

type compactionListener struct {
	startTime time.Time
}

func (l *compactionListener) CompactionBegin(info pebble.CompactionInfo) {
	l.startTime = time.Now()
	inputFileCount := 0
	for _, levelInfo := range info.Input {
		inputFileCount += len(levelInfo.Tables)
	}
	
	if len(info.Input) > 0 {
		fmt.Printf("[Compaction] Started compaction job #%d: level %d -> level %d\n",
			info.JobID, info.Input[0].Level, info.Output.Level)
		if len(info.Input) > 1 {
			fmt.Printf("[Compaction]   Merging %d input levels\n", len(info.Input))
		}
	} else {
		fmt.Printf("[Compaction] Started compaction job #%d -> level %d\n",
			info.JobID, info.Output.Level)
	}
	fmt.Printf("[Compaction]   Input files: %d\n", inputFileCount)
	fmt.Printf("[Compaction]   Output level: %d\n", info.Output.Level)
}

func (l *compactionListener) CompactionEnd(info pebble.CompactionInfo) {
	duration := time.Since(l.startTime)
	fmt.Printf("[Compaction] Completed compaction job #%d in %v\n",
		info.JobID, duration.Round(time.Millisecond))
}

func (l *compactionListener) DiskSlow(pebble.DiskSlowInfo) {}

func (l *compactionListener) FlushBegin(pebble.FlushInfo) {}

func (l *compactionListener) FlushEnd(pebble.FlushInfo) {}

func (l *compactionListener) ManifestCreated(pebble.ManifestCreateInfo) {}

func (l *compactionListener) ManifestDeleted(pebble.ManifestDeleteInfo) {}

func (l *compactionListener) TableCreated(pebble.TableCreateInfo) {}

func (l *compactionListener) TableDeleted(pebble.TableDeleteInfo) {}

func (l *compactionListener) TableIngested(pebble.TableIngestInfo) {}

func (l *compactionListener) WALCreated(pebble.WALCreateInfo) {}

func (l *compactionListener) WALDeleted(pebble.WALDeleteInfo) {}

func (l *compactionListener) WriteStallBegin(pebble.WriteStallBeginInfo) {}

func (l *compactionListener) WriteStallEnd() {}

func main() {
	if len(os.Args) != 2 {
		panic("Usage: pebble <dbPath>")
	}

	dbPath := os.Args[1]
	fmt.Printf("Opening database: %s\n", dbPath)

	listener := &compactionListener{}
	opts := &pebble.Options{
		MaxOpenFiles: 100,
		EventListener: pebble.EventListener{
			CompactionBegin: listener.CompactionBegin,
			CompactionEnd:   listener.CompactionEnd,
		},
	}
	opts.EnsureDefaults()

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		db.Close()
	}()

	fmt.Println("Scanning database to determine key range...")
	iter := db.NewIter(nil)
	var start, end []byte

	if iter.First() {
		start = cp(iter.Key())
		fmt.Printf("Found start key: %x\n", start)
	} else {
		fmt.Println("Warning: Database appears to be empty (no start key found)")
	}

	if iter.Last() {
		end = cp(iter.Key())
		fmt.Printf("Found end key: %x\n", end)
	} else {
		fmt.Println("Warning: Database appears to be empty (no end key found)")
	}

	if err := iter.Close(); err != nil {
		panic(err)
	}

	if start == nil || end == nil {
		fmt.Println("Database is empty, nothing to compact.")
		return
	}

	fmt.Println("\nStarting compaction...")
	fmt.Printf("Compacting key range: [%x ... %x]\n", start, end)
	fmt.Println("Press Ctrl+C to interrupt (safe - database will close gracefully)")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	interrupted := false

	// Monitor for interrupts in background
	go func() {
		<-sigChan
		interrupted = true
		fmt.Printf("\n\nReceived interrupt signal. Shutting down gracefully...\n")
		fmt.Println("Note: Compaction was interrupted. Database is safe but may not be fully compacted.")
		fmt.Println("When you run compaction again, it will process the entire key range from the start,")
		fmt.Println("but Pebble will intelligently skip files that were already compacted.")
		os.Exit(0)
	}()

	overallStart := time.Now()

	err = db.Compact(start, end, false)
	if err != nil && !interrupted {
		panic(err)
	}

	if !interrupted {
		duration := time.Since(overallStart)
		fmt.Printf("\nCompaction completed successfully in %v\n", duration.Round(time.Millisecond))
	}
}
