package core

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/fsnotify.v1"

	"io/ioutil"

	_ "github.com/mattn/go-sqlite3" // SQLite3 driver will register itself
	"github.com/traherom/fsnotifydeep"
	"github.com/traherom/gocrypt"
	"github.com/traherom/gocrypt/aes"
	"github.com/traherom/gocrypt/hash"
	"github.com/traherom/memstream"
	"golang.org/x/crypto/pbkdf2"
)

// pbkdf2Iterations is used for key import and export (by password)
const pbkdf2Iterations = 100000

// SyncInfo stores information common to all files in a given Sync
type SyncInfo struct {
	localBase  string            // Base path of the "local" directory, where files are unencrypted
	remoteBase string            // Base path of the "remote" directory, where files are encrypted
	keys       *gocrypt.KeyCombo // Crypto and auth keys for protected file headers

	// Runtime-specific info
	db           *sql.DB   // Connection to sync SQLite database
	changesReady chan bool // If a user believes the sync has changes waiting to be processed, they should signal via this channel
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
	sync.changesReady = make(chan bool)

	if sync.localBase, err = filepath.Abs(localPath); err != nil {
		return nil, &ErrSync{"Unable to get absolute local path", err}
	}

	if sync.remoteBase, err = sync.Get(remotePathKey); err != nil {
		return nil, &ErrSync{"Remote path not set", err}
	}
	if sync.remoteBase, err = filepath.Abs(filepath.FromSlash(sync.remoteBase)); err != nil {
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

	sync.keys = &gocrypt.KeyCombo{CryptoKey: cKey, AuthKey: aKey}

	// Make sure everything loaded correctly
	if err = sync.sanityCheckConfig(); err != nil {
		return nil, err
	}

	log.Println("Cleaning sync")
	sync.Clean()

	return sync, nil
}

// CreateSync initializes and loads a new sync between the given local and remote paths.
func CreateSync(localPath string, remotePath string, keys *gocrypt.KeyCombo) (sync *SyncInfo, err error) {
	// Create any directories necessary
	dbPath := filepath.Join(localPath, settingsDir, dbFileName)
	if err = os.MkdirAll(filepath.Dir(dbPath), 0777); err != nil {
		return nil, &ErrSync{"Unable to create local path", err}
	}
	if err = os.MkdirAll(remotePath, 0777); err != nil {
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
	sync.changesReady = make(chan bool)

	if sync.localBase, err = filepath.Abs(localPath); err != nil {
		return nil, &ErrSync{"Unable to get absolute local path", err}
	}
	if sync.remoteBase, err = filepath.Abs(remotePath); err != nil {
		return nil, &ErrSync{"Unable to get absolute remote path", err}
	}

	sync.keys = keys

	remoteAbs, err := filepath.Abs(remotePath)
	if err != nil {
		return nil, &ErrSync{"Unable to determine absolute remote path", err}
	}

	sync.Set(remotePathKey, filepath.ToSlash(remoteAbs))
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

// ProcessChange notifies the sync queue monitor that changes are ready to be pulled
func (s *SyncInfo) ProcessChange() {
	s.changesReady <- true
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
						                        unseen_local BOOLEAN NOT NULL DEFAULT 0,
						                        unseen_remote BOOLEAN NOT NULL DEFAULT 0);
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

// ExportKeys exports this sync's keys to the given path, protecting them with
// the given password.
func (s *SyncInfo) ExportKeys(outPath, pw string) error {
	salt, err := gocrypt.SecureBytes(aes.KeyLength)
	if err != nil {
		return &ErrSync{"Unable to get salt for export", err}
	}

	exportKeys := generatePbkdf2KeyCombo(pw, salt)

	origBuf := memstream.New()
	origBuf.Write(s.Keys().CryptoKey)
	origBuf.Write(s.Keys().AuthKey)
	origBuf.Rewind()

	encryptedBuf := memstream.New()
	encryptedBuf.Write(salt)
	_, _, err = aes.Encrypt(origBuf, encryptedBuf, exportKeys)
	if err != nil {
		return &ErrSync{"Unable to encrypt keys", err}
	}

	encoded := gocrypt.BytesToB64(encryptedBuf.Bytes())
	err = ioutil.WriteFile(outPath, []byte(encoded), 0770)
	if err != nil {
		return &ErrSync{"Unable to open key file", err}
	}

	return nil
}

// ImportKeys imports the keys from the given file, replacing the current keys for this sync
func (s *SyncInfo) ImportKeys(inPath, pw string) error {
	encoded, err := ioutil.ReadFile(inPath)
	if err != nil {
		return &ErrSync{"Unable to open key file", err}
	}

	encrypted, err := gocrypt.BytesFromB64(string(encoded))
	if err != nil {
		return &ErrSync{"Unable to decode key file", err}
	}

	salt := encrypted[:aes.KeyLength]
	exportKeys := generatePbkdf2KeyCombo(pw, salt)

	decrypted := memstream.New()
	_, cnt, err := aes.Decrypt(bytes.NewBuffer(encrypted[aes.KeyLength:]), decrypted, exportKeys)
	if err != nil {
		return &ErrSync{"Unable to decrypt keys", err}
	}

	if cnt != int64(aes.KeyLength*2) {
		return &ErrSync{"Keys read, but do not appear to be the correct format", nil}
	}

	decrypted.Rewind()

	cryptoKey := gocrypt.Key(make([]byte, aes.KeyLength))
	var read int
	if read, err = decrypted.Read(cryptoKey); err != nil && err != io.EOF {
		return &ErrSync{"Failed to read crypto key", err}
	}
	cryptoKey = cryptoKey[:read]

	authKey := gocrypt.Key(make([]byte, aes.KeyLength))
	if read, err = decrypted.Read(authKey); err != nil && err != io.EOF {
		return &ErrSync{"Failed to read auth key", err}
	}
	authKey = authKey[:read]

	if len(cryptoKey) != aes.KeyLength || len(authKey) != aes.KeyLength {
		return &ErrSync{"Keys read, but they do not appear to be long enough", nil}
	}

	s.Set(cryptoKeyKey, cryptoKey.String())
	s.Set(authKeyKey, authKey.String())

	return nil
}

func generatePbkdf2KeyCombo(pw string, salt []byte) *gocrypt.KeyCombo {
	key := pbkdf2.Key([]byte(pw), salt, pbkdf2Iterations, aes.KeyLength*2, sha256.New)
	return &gocrypt.KeyCombo{
		CryptoKey: key[:aes.KeyLength],
		AuthKey:   key[aes.KeyLength:],
	}
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
func (s *SyncInfo) Monitor(changes chan *Change, errors chan error, die chan bool) {
	childDie := make(chan bool)
	var wg sync.WaitGroup
	defer func() {
		close(childDie)
		log.Println("Waiting for all monitor subprocessors to end")
		wg.Wait()
		log.Println("All monitoring ended")
	}()

	// Ensure we're ready for the change processor
	prepareChangeQueue(s)

	// Monitor for new changes anywhere in tree
	localWatcher, err := watcherForDir(s.LocalBase())
	if err != nil {
		log.Printf("Unable to start filesystem monitor: %v\n", err)
		return
	}
	defer localWatcher.Close()

	remoteWatcher, err := watcherForDir(s.RemoteBase())
	if err != nil {
		log.Printf("Unable to start filesystem monitor: %v\n", err)
		return
	}
	defer remoteWatcher.Close()

	// Scan for changes since last run
	wg.Add(1)
	go func() {
		s.initialScan(changes, errors, childDie)
		wg.Done()
	}()

	// Handle realtime changes
watchLoop:
	for {
		select {
		case evt := <-localWatcher.Events:
			newChange := new(Change)
			newChange.sync = s
			newChange.localPath, err = filepath.Rel(s.LocalBase(), evt.Name)
			if err != nil {
				errors <- &ErrSync{fmt.Sprintf("Unable to compute relative path for %v and %v\n", s.LocalBase(), evt.Name), err}
				continue
			}

			switch evt.Op {
			case fsnotify.Create:
				newChange.changeType = LocalAdd
			case fsnotify.Remove:
				newChange.changeType = LocalDelete
			case fsnotify.Write:
				newChange.changeType = LocalChange
			default:
				errors <- &ErrSync{fmt.Sprintf("fsnotify event type %v should have been filtered", evt.Op), nil}
				continue
			}

			// Get current remote path
			/*if evt.Op != fsnotify.Create {
				entry, err := GetCacheEntryViaLocal(s, newChange.localPath)
				if err != nil && err != sql.ErrNoRows {
					errors <- &ErrSync{fmt.Sprintf("Unable to get remote path for %v\n", newChange.localPath), err}
				}
				if entry != nil {
					newChange.remotePath = entry.RemotePath()
					newChange.cacheEntry = entry
				}
			}*/

			changes <- newChange

		case evt := <-remoteWatcher.Events:
			newChange := new(Change)
			newChange.sync = s
			newChange.remotePath, err = filepath.Rel(s.RemoteBase(), evt.Name)
			if err != nil {
				errors <- &ErrSync{fmt.Sprintf("Unable to compute relative path for %v and %v\n", s.RemoteBase(), evt.Name), err}
				continue
			}

			switch evt.Op {
			case fsnotify.Create:
				newChange.changeType = RemoteAdd
			case fsnotify.Remove:
				newChange.changeType = RemoteDelete
			case fsnotify.Write:
				newChange.changeType = RemoteChange
			default:
				errors <- &ErrSync{fmt.Sprintf("fsnotify event type %v should have been filtered", evt.Op), nil}
			}

			// Get current remote path
			/*if evt.Op != fsnotify.Create {
				entry, err := GetCacheEntryViaRemote(s, newChange.remotePath)
				if err != nil && err != sql.ErrNoRows {
					errors <- &ErrSync{fmt.Sprintf("Unable to get local path for %v\n", newChange.remotePath), err}
				}
				if entry != nil {
					newChange.remotePath = entry.RemotePath()
					newChange.cacheEntry = entry
				}
			}*/

			changes <- newChange

		case err := <-localWatcher.Errors:
			log.Println("Error during monitoring local:", err)
		case err := <-remoteWatcher.Errors:
			log.Println("Error during monitoring remote:", err)
		case <-die:
			break watchLoop
		}
	}

	log.Printf("Monitor for %v quitting\n", s.LocalBase())
}

// initialScan looks for changes that have occured since the last time
// this sync was monitored
func (s *SyncInfo) initialScan(changes chan *Change, errors chan error, die chan bool) {
	if err := s.MarkAllEntriesUnseen(); err != nil {
		errors <- err
		return
	}

	var wg sync.WaitGroup

	// Scan local
	wg.Add(1)
	go func() {
		filepath.Walk(s.LocalBase(), func(path string, info os.FileInfo, err error) error {
			if strings.Contains(path, ".syncer") {
				return nil
			}
			if info.IsDir() {
				return nil
			}

			path, err = filepath.Rel(s.LocalBase(), path)
			if err != nil {
				realErr := &ErrSync{"Unable to get relative path during initial scan", err}
				errors <- realErr
				return realErr
			}

			entry, err := GetCacheEntryViaLocal(s, path)
			if err == sql.ErrNoRows {
				// New local file
				changes <- &Change{
					localPath:  path,
					changeType: LocalAdd,
					sync:       s,
				}
				return nil
			} else if err != nil {
				// Any other error means a real issue
				errors <- &ErrSync{"Error during initial scan", err}
				return nil
			}

			if err = entry.MarkLocalSeen(); err != nil {
				errors <- err
			}

			// Is it changed?
			sum, err := hash.Sha256File(entry.AbsLocalPath())
			if err != nil {
				errors <- &ErrSync{"Error checking for change", err}
				return nil
			}

			if bytes.Compare(entry.LocalHash(), sum) != 0 {
				// Changed
				changes <- &Change{
					localPath:  entry.LocalPath(),
					remotePath: entry.RemotePath(),
					changeType: LocalChange,
					cacheEntry: entry,
					sync:       s,
				}
			}

			return nil
		})

		// If we didn't see a file, that means it was removed
		unseen, err := s.GetUnseenLocalEntries()
		if err != nil {
			errors <- &ErrSync{"Unable to check for unseen entries", err}
			return
		}

		for el := unseen.Front(); el != nil; el = el.Next() {
			e, ok := el.Value.(*CacheEntry)
			if !ok {
				panic("GetUnseenLocalEntries returned a non-CacheEntry value")
			}

			changes <- &Change{
				localPath:  e.LocalPath(),
				remotePath: e.RemotePath(),
				cacheEntry: e,
				changeType: LocalDelete,
				sync:       s,
			}
		}

		log.Println("Initial local scan complete")
		wg.Done()
	}()

	// Scan remote
	wg.Add(1)
	go func() {
		filepath.Walk(s.RemoteBase(), func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ProtFileExt) {
				return nil
			}

			path, err = filepath.Rel(s.RemoteBase(), path)
			if err != nil {
				realErr := &ErrSync{"Unable to get relative path during initial scan", err}
				errors <- realErr
				return realErr
			}

			entry, err := GetCacheEntryViaRemote(s, path)
			if err == sql.ErrNoRows {
				// New remote file
				changes <- &Change{
					remotePath: path,
					changeType: RemoteAdd,
					sync:       s,
				}
				return nil
			} else if err != nil {
				// Any other error means a real issue
				errors <- &ErrSync{"Error during initial scan", err}
				return nil
			}

			if err = entry.MarkRemoteSeen(); err != nil {
				errors <- err
			}

			// Is it changed?
			sum, err := hash.Sha256File(entry.AbsRemotePath())
			if err != nil {
				errors <- &ErrSync{"Error checking for change", err}
				return nil
			}

			if bytes.Compare(entry.RemoteHash(), sum) != 0 {
				// Changed
				changes <- &Change{
					remotePath: entry.RemotePath(),
					localPath:  entry.LocalPath(),
					changeType: RemoteChange,
					cacheEntry: entry,
					sync:       s,
				}
			}

			return nil
		})

		// If we didn't see a file, that means it was removed
		unseen, err := s.GetUnseenRemoteEntries()
		if err != nil {
			errors <- &ErrSync{"Unable to check for unseen entries", err}
			return
		}

		for el := unseen.Front(); el != nil; el = el.Next() {
			e, ok := el.Value.(*CacheEntry)
			if !ok {
				panic("GetUnseenRemoteEntries returned a non-CacheEntry value")
			}

			changes <- &Change{
				localPath:  e.LocalPath(),
				remotePath: e.RemotePath(),
				cacheEntry: e,
				changeType: RemoteDelete,
				sync:       s,
			}
		}

		log.Println("Initial remote scan complete")
		wg.Done()
	}()

	wg.Wait()
	log.Println("Initial scan complete")
}

// Clean removes unneeded temp files, database entries, etc from the sync
func (s *SyncInfo) Clean() error {
	return cleanChangeQueue(s)
}

// watcherForDir creates a new filesystem watcher that monitors everything
// under the given root for creates, modifies, and deletes
func watcherForDir(root string) (*fsnotifydeep.Watcher, error) {
	watcher, err := fsnotifydeep.NewWatcher()
	if err != nil {
		return nil, err
	}

	watcher.Filter(func(evt fsnotify.Event) bool {
		// Only create, remove, and write on NOT .syncer directories
		return ((evt.Op == fsnotify.Create ||
			evt.Op == fsnotify.Remove ||
			evt.Op == fsnotify.Write) &&
			!strings.Contains(evt.Name, settingsDir))
	})

	err = watcher.Add(root)
	if err != nil {
		watcher.Close()
		return nil, err
	}

	return watcher, nil
}
