package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/traherom/gocrypt/aes"
)

func main() {
	const localDir = "run/local"
	const remoteDir = "run/remote"

	var testSync *SyncInfo
	if _, err := os.Stat(filepath.Dir(localDir)); err != nil {
		fmt.Println("Creating sync")
		keys, err := aes.NewKeyCombo()
		if err != nil {
			fmt.Printf("Failed to create keys for new sync: %v\n", err)
			return
		}

		testSync, err = CreateSync(localDir, remoteDir, keys)
		if err != nil {
			fmt.Printf("Failed to create sync: %v\n", err)
			return
		}
	} else {
		fmt.Println("Loading sync")
		testSync, err = LoadSync(localDir)
		if err != nil {
			fmt.Printf("Failed to load sync: %v\n", err)
			return
		}
	}

	// Change processor
	var wg sync.WaitGroup
	die := make(chan bool)
	changes := make(chan Change)
	errors := make(chan error)

	wg.Add(1)
	go func() {
		ChangeQueueManager(changes, errors, die)
		wg.Done()
	}()

	// Start monitoring and wait for enter
	wg.Add(1)
	go func() {
		testSync.Monitor(changes, errors, die)
		wg.Done()
	}()

	fmt.Println("Monitoring started, press <enter> to end")
	end := make(chan bool)
	go func() {
		fmt.Scanln()
		close(end)
	}()

	// UI interaction
mainLoop:
	for {
		select {
		case err := <-errors:
			fmt.Println("Error:", err)
		case <-end:
			break mainLoop
		}
	}

	// All done
	close(die)
	wg.Wait()
}
