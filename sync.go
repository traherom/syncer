package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/fsnotify.v1"

	_ "github.com/mattn/go-sqlite3" // SQLite3 driver
	"github.com/traherom/fsnotifydeep"
	"github.com/traherom/gocrypt"
	"github.com/traherom/gocrypt/aes"
)

// SyncInfo stores information common to all files in a given Sync
type SyncInfo struct {
	localBase  string            // Base path of the "local" directory, where files are unencrypted
	remoteBase string            // Base path of the "remote" directory, where files are encrypted
	keys       *gocrypt.KeyCombo // Crypto and auth keys for protected file headers

	// Runtime-specific info
	db *sql.DB // Connection to sync SQLite database
}

// ErrSync is a universal error type for all sync issues
type ErrSync struct {
	msg   string // Error message
	inner error  // Inner error that caused issue
}

func (e *ErrSync) Error() string {
	if e.inner == nil {
		return fmt.Sprintf("sync: %v", e.msg)
	}

	return fmt.Sprintf("sync: %v: %v", e.msg, e.inner.Error())
}

// Directory under local path where sync settings, temp files, etc should be stored
const settingsDir = ".syncer"

// Name af dotabase file for primary sync settings storage
const dbFileName = "sync.sqlite"

// Key names for primary settings
const remotePathKey = "remote"
const cryptoKeyKey = "cryptokey"
const authKeyKey = "authkey"

// LoadSync loads the sync based on the settings found in localPath
func LoadSync(localPath string) (*SyncInfo, error) {
	// Only open pre-existing syncs. sql.Open would indiscrimately create an
	// empty database if we didn't check this in advance
	dbPath := filepath.Join(localPath, settingsDir, dbFileName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, &ErrSync{fmt.Sprintf("'%v' does not appear to be a valid sync: no settings directory found", localPath), err}
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Load remainder of settings
	sync := new(SyncInfo)
	sync.db = db

	if sync.localBase, err = filepath.Abs(localPath); err != nil {
		return nil, &ErrSync{"Unable to get absolute local path", err}
	}

	if sync.remoteBase, err = sync.Get(remotePathKey); err != nil {
		return nil, &ErrSync{"Remote path not set", err}
	}

	if sync.remoteBase, err = filepath.Abs(sync.remoteBase); err != nil {
		return nil, &ErrSync{"Unable to get absolute remote path", err}
	}

	cKeyStr, err := sync.Get(cryptoKeyKey)
	if err != nil {
		return nil, &ErrSync{"Encryption key not set", err}
	}

	aKeyStr, err := sync.Get(authKeyKey)
	if err != nil {
		return nil, &ErrSync{"Authentication key not set", err}
	}

	cKey, err := gocrypt.KeyFromString(cKeyStr)
	if err != nil {
		return nil, &ErrSync{"Failed to convert encryption key", err}
	}
	aKey, err := gocrypt.KeyFromString(aKeyStr)
	if err != nil {
		return nil, &ErrSync{"Failed to convert authentication key", err}
	}

	sync.keys = &gocrypt.KeyCombo{cKey, aKey} // go vet complains about unkeyed fields, ignore

	// Make sure everything loaded correctly
	if err = sync.sanityCheckConfig(); err != nil {
		return nil, err
	}

	return sync, nil
}

// CreateSync initializes and loads a new sync between the given local and remote paths.
// Directories are created with restrictive permissions by default (0700)
func CreateSync(localPath string, remotePath string, keys *gocrypt.KeyCombo) (sync *SyncInfo, err error) {
	// Create any directories necessary
	dbPath := filepath.Join(localPath, settingsDir, dbFileName)
	if err = os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, &ErrSync{"Unable to create local path", err}
	}
	if err = os.MkdirAll(remotePath, 0700); err != nil {
		return nil, &ErrSync{"Unable to create remote path", err}
	}

	// Create database schema
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, &ErrSync{"Unable to create database", err}
	}

	initDbSchema(db)

	// Initial settings
	sync = new(SyncInfo)
	sync.db = db
	if sync.localBase, err = filepath.Abs(localPath); err != nil {
		return nil, &ErrSync{"Unable to get absolute local path", err}
	}
	if sync.remoteBase, err = filepath.Abs(remotePath); err != nil {
		return nil, &ErrSync{"Unable to get absolute remote path", err}
	}

	sync.keys = keys

	sync.Set(remotePathKey, filepath.Clean(remotePath))
	sync.Set(cryptoKeyKey, keys.CryptoKey.String())
	sync.Set(authKeyKey, keys.AuthKey.String())

	// Any issues?
	if err = sync.sanityCheckConfig(); err != nil {
		return nil, err
	}

	return sync, nil
}

// sanityCheckConfig ensures that the sync settings present make sense. For example,
// the local and remote paths should exist.
func (s *SyncInfo) sanityCheckConfig() error {
	// Keys
	if s.keys == nil {
		return &ErrSync{"Encryption keys not loaded", nil}
	}
	if len(s.keys.CryptoKey) != aes.KeyLength || len(s.keys.AuthKey) != aes.KeyLength {
		return &ErrSync{fmt.Sprintf("Encryption key(s) not %v bits long", aes.KeyLength*8), nil}
	}

	// Paths
	if _, err := os.Stat(s.localBase); os.IsNotExist(err) {
		return &ErrSync{"Local path does not exist", err}
	}

	if _, err := os.Stat(s.remoteBase); os.IsNotExist(err) {
		return &ErrSync{"Remote path does not exist", err}
	}

	// database
	if s.db == nil {
		return &ErrSync{"Database not loaded", nil}
	}

	return nil
}

// Creates database schema as needed
func initDbSchema(db *sql.DB) error {
	schema := `
	-- Settings
	CREATE TABLE IF NOT EXISTS settings (name TEXT NOT NULL, value TEXT NULL);
  CREATE INDEX IF NOT EXISTS settings_lookup ON settings (name);

  -- Files
	CREATE TABLE IF NOT EXISTS cache (id INTEGER PRIMARY KEY,
                                    rel_local_path TEXT NOT NULL,
						                        rel_remote_path TEXT NOT NULL,
						                        remote_hash BLOB NOT NULL,
						                        local_hash BLOB NOT NULL,
						                        seen_in_search BOOLEAN NOT NULL DEFAULT 0);
	`

	_, err := db.Exec(schema)
	return err
}

// Close terminates any outstanding sync data in a clean way
func (s *SyncInfo) Close() {
	s.db.Close()
}

// LocalBase returns the absolute path to the base directory of this sync's
// local (unencrypted) directory
func (s *SyncInfo) LocalBase() string {
	return s.localBase
}

// RemoteBase returns the absolute path to the base directory of this sync's
// remote (encrypted) directory
func (s *SyncInfo) RemoteBase() string {
	return s.remoteBase
}

// Keys returns a copy of the keys for this sync's metadata
func (s *SyncInfo) Keys() gocrypt.KeyCombo {
	return *s.keys
}

// Get retrieves the named setting from the database or "" if it cannot be found.
// If not found, err will be set to sql.ErrNoRows
func (s *SyncInfo) Get(name string) (value string, err error) {
	return s.GetDefault(name, "")
}

// GetDefault retrieves the named setting from the database. If the setting does not
// exist, returns defaultVal instead
func (s *SyncInfo) GetDefault(name string, defaultVal string) (value string, err error) {
	row := s.db.QueryRow("SELECT value FROM settings WHERE name=?", name)
	err = row.Scan(&value)
	if err != nil {
		return defaultVal, err
	}

	return value, nil
}

// Set saves the name->value setting pair into the database. If the setting already
// exists, it is replaced with the new value and the old value is returned. If it
// did not exist, "" is returned as the previous value.
func (s *SyncInfo) Set(name string, value string) (previous string, err error) {
	previous, err = s.Get(name)

	switch {
	case err == sql.ErrNoRows: // Did not exist previously
		_, err = s.db.Exec("INSERT INTO settings (name, value) VALUES (?, ?)", name, value)
	case err != nil: // Real error
		return "", err
	default: // Update
		_, err = s.db.Exec("UPDATE settings SET value=? WHERE name=?", value, name)
	}

	return
}

// Monitor begins running the entire Sync monitoring suite: checking for new changes
// since the last run, watching for new realtime changes, and updating files as
// needed. As changes are seen, they will be pushed to the changes channel. This function
// does not make any filesystem changes directly.
//
// This function expects to execute as a goroutine. Using the accepted channel,
// passing in true will result in the monitor cleanly exiting its subcomponents.
func (s *SyncInfo) Monitor(changes chan Change, errors chan error, die chan bool) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	// Ensure we're ready for the change processor
	PrepareChangeQueue(s)

	// Monitor for new changes anywhere in tree
	localWatcher, err := watcherForDir(s.LocalBase())
	if err != nil {
		fmt.Printf("Unable to start filesystem monitor: %v\n", err)
		return
	}
	defer localWatcher.Close()

	remoteWatcher, err := watcherForDir(s.RemoteBase())
	if err != nil {
		fmt.Printf("Unable to start filesystem monitor: %v\n", err)
		return
	}
	defer remoteWatcher.Close()

	// Scan for changes since last run
	wg.Add(1)
	go func() {
		s.initialScan(changes, die)
		wg.Done()
	}()

	// Handle realtime changes
watchLoop:
	for {
		select {
		case evt := <-localWatcher.Events:
			fmt.Println(evt)
		case evt := <-remoteWatcher.Events:
			fmt.Println(evt)
		case err := <-localWatcher.Errors:
			fmt.Println("Error during monitoring local:", err)
		case err := <-remoteWatcher.Errors:
			fmt.Println("Error during monitoring remote:", err)
		case <-die:
			break watchLoop
		}
	}

	fmt.Printf("Monitor for %v quitting\n", s.LocalBase())
}

// initialScan looks for changes that have occured since the last time
// this sync was monitored
func (s *SyncInfo) initialScan(changes chan Change, die chan bool) {

}

// watcherForDir creates a new filesystem watcher that monitors everything
// under the given root for creates, modifies, and deletes
func watcherForDir(root string) (*fsnotifydeep.Watcher, error) {
	watcher, err := fsnotifydeep.NewWatcher()
	if err != nil {
		return nil, err
	}

	watcher.Filter(func(evt fsnotify.Event) bool {
		return evt.Op == fsnotify.Create || evt.Op == fsnotify.Remove || evt.Op == fsnotify.Write
	})

	err = watcher.Add(root)
	if err != nil {
		watcher.Close()
		return nil, err
	}

	return watcher, nil
}
