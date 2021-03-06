package main

import (
	"fmt"
	"os"
	"path/filepath"
	syncPkg "sync"

	"github.com/codegangsta/cli"
	"github.com/traherom/gocrypt/aes"
	"github.com/traherom/syncer/core"
	"log"
)

func main() {
	app := cli.NewApp()
	app.Name = "syncer"
	app.Usage = "sync a local folder with an protected cloud-synced folder"
	app.Commands = []cli.Command{
		{
			Name:   "init",
			Usage:  "Create a new sync between a local and remote directory",
			Action: initSync,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "local, l",
					Usage: "Path to local directory. Will be convected an absolute path.",
					Value: ".",
				},
				cli.StringFlag{
					Name:  "remote, r",
					Usage: "Path to remote directory. Will be converted an absolute path.",
				},
			},
		},
		{
			Name:   "monitor",
			Usage:  "Monitor syncs for changes",
			Action: monitorSync,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "local, l",
					Usage: "Path to local directory.",
					Value: ".",
				},
			},
		},
		{
			Name:   "export-keys",
			Usage:  "Exports the keys for this sync to allow sharing with another user",
			Action: exportKeys,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "local, l",
					Usage: "Path to local directory holing the sync.",
					Value: ".",
				},
				cli.StringFlag{
					Name:  "out, o",
					Usage: "Output location for key file. Defaults to <remote dir>/exported.keys",
					Value: "",
				},
				cli.StringFlag{
					Name:  "password, pw",
					Usage: "Password to encrypt the key export file with",
					Value: "",
				},
			},
		},
		{
			Name:   "import-keys",
			Usage:  "Imports new keys for this sync",
			Action: importKeys,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "local, l",
					Usage: "Path to local directory holding the sync.",
					Value: ".",
				},
				cli.StringFlag{
					Name:  "in, i",
					Usage: "Location of key file. Defaults to <remote dir>/exported.keys",
					Value: "",
				},
				cli.StringFlag{
					Name:  "password, pw",
					Usage: "Password to decrypt the key file with",
					Value: "",
				},
			},
		},
	}

	app.Run(os.Args)
}

func initSync(c *cli.Context) {
	localDir := c.String("local")
	remoteDir := c.String("remote")

	// Command check
	if localDir == "" {
		log.Println("Local directory must be set to create sync")
		return
	}
	if remoteDir == "" {
		log.Println("Remote directory must be set to create sync")
		return
	}

	log.Printf("Creating sync %v <-> %v\n", localDir, remoteDir)
	keys, err := aes.NewKeyCombo()
	if err != nil {
		fmt.Printf("Failed to create keys for new sync: %v\n", err)
		return
	}

	_, err = core.CreateSync(localDir, remoteDir, keys)
	if err != nil {
		fmt.Printf("Failed to create sync: %v\n", err)
		return
	}
}

func monitorSync(c *cli.Context) {
	localDir := c.String("local")

	// Command check
	if localDir == "" {
		log.Println("Local directory must be set to monitor")
		return
	}

	log.Println("Loading sync")
	sync, err := core.LoadSync(localDir)
	if err != nil {
		fmt.Printf("Failed to load sync: %v\n", err)
		return
	}

	// Change processor
	var wg syncPkg.WaitGroup
	die := make(chan bool)
	changes := make(chan *core.Change)
	completed := make(chan *core.Change)
	errors := make(chan error)

	wg.Add(1)
	go func() {
		core.ChangeQueueManager(changes, completed, errors, die)
		wg.Done()
	}()

	// Start monitoring and wait for enter
	wg.Add(1)
	go func() {
		sync.Monitor(changes, errors, die)
		wg.Done()
	}()

	// If all our children die, end the main program
	end := make(chan bool)
	go func() {
		wg.Wait()
		close(end)
		log.Println("All children have died, exiting")
	}()

	log.Println("Monitoring started")
	/*go func() {
		fmt.Scanln()
		close(end)
	}()*/

	// UI interaction
mainLoop:
	for {
		select {
		case change := <-completed:
			log.Println("Completed", change)
		case err := <-errors:
			log.Println("Error:", err)
		case <-end:
			break mainLoop
		}
	}

	// TODO timeout on waiting?
	log.Println("Requesting that handlers close")
	close(die)
	wg.Wait()
	log.Println("Syncer ended cleanly")
}

func exportKeys(c *cli.Context) {
	localDir := c.String("local")
	password := c.String("password")
	//keyName := c.String("gpg")
	outPath := c.String("out")

	// Command check
	if localDir == "" {
		log.Println("Local directory must be set to export keys")
		return
	}
	if password == "" {
		log.Println("Password to protect keys with must be given")
		return
	}

	log.Println("Loading sync")
	sync, err := core.LoadSync(localDir)
	if err != nil {
		fmt.Printf("Failed to load sync: %v\n", err)
		return
	}

	if outPath == "" {
		outPath = filepath.Join(sync.RemoteBase(), "export.keys")
	}

	if err := sync.ExportKeys(outPath, password); err != nil {
		log.Println("Export failed:", err)
		return
	}

	log.Println("Keys exported to", outPath)
}

func importKeys(c *cli.Context) {
	localDir := c.String("local")
	password := c.String("password")
	//keyName := c.String("gpg")
	inPath := c.String("in")

	// Command check
	if localDir == "" {
		log.Println("Local directory must be set to import keys")
		return
	}
	if password == "" {
		log.Println("Password to decrypt keys with must be given")
		return
	}

	log.Println("Loading sync")
	sync, err := core.LoadSync(localDir)
	if err != nil {
		fmt.Printf("Failed to load sync: %v\n", err)
		return
	}

	if inPath == "" {
		inPath = filepath.Join(sync.RemoteBase(), "export.keys")
	}

	if err := sync.ImportKeys(inPath, password); err != nil {
		log.Println("Import failed:", err)
		return
	}

	log.Println("Keys imported from", inPath)
}
