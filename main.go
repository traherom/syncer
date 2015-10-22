package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/traherom/gocrypt/aes"
)

func main() {
	localDir := filepath.FromSlash("run/local")
	remoteDir := filepath.FromSlash("run/remote")

	var testSync *SyncInfo
	if _, err := os.Stat(filepath.Dir(localDir)); err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("Error checking if sync exists:", err)
			return
		}

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
	changes := make(chan *Change)
	completed := make(chan *Change)
	errors := make(chan error)

	wg.Add(1)
	go func() {
		ChangeQueueManager(changes, completed, errors, die)
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
	/*go func() {
		fmt.Scanln()
		close(end)
	}()*/

	// UI interaction
mainLoop:
	for {
		select {
		case change := <-completed:
			fmt.Println("Completed", change)
		case err := <-errors:
			fmt.Println("Error:", err)
		case <-end:
			break mainLoop
		}
	}

	// TODO timeout on waiting?
	fmt.Println("Requesting that handlers close")
	close(die)
	wg.Wait()
	fmt.Println("Syncer ended cleanly")
}
