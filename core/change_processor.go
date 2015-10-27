package core

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// ErrProcessor present any errors occuring change processing
type ErrProcessor struct {
	Msg   string
	Inner error
}

func (e *ErrProcessor) Error() string {
	if e.Inner == nil {
		return fmt.Sprintf("change processor: %v", e.Msg)
	}

	return fmt.Sprintf("change processor: %v: %v", e.Msg, e.Inner)
}

// ChangeType represents the type of change which has occured. Values for this
// type should be one of LocalAdd, LocalDelete, LocalChange, RemoteAdd,
// RemoteDelete, or RemoteChange
type ChangeType int

// Valid values for a ChangeType
const (
	LocalAdd ChangeType = iota
	LocalDelete
	LocalChange
	RemoteAdd
	RemoteDelete
	RemoteChange
)

// Change represents a file change on either the remote or local side of a sync
// It is general. The inclusion of the specific SyncInfo it belongs to allows
// a change processor to be shared amongst multiple syncs
type Change struct {
	localPath     string      // Path to changed file relative to sync.LocalBase()
	remotePath    string      // Path to changed file relative to sync.RemoteBase()
	changeType    ChangeType  // Type of change
	cacheEntry    *CacheEntry // If this change involves previously known files, this may be populated
	protectedFile *Header     // Header info of the protected file this change is tied to
	sync          *SyncInfo   // Sync this change is a part of
	id            int         // Internal database id of this change
}

func (c *Change) String() string {
	switch c.changeType {
	case LocalAdd:
		return fmt.Sprintf("local add: %v", c.LocalPath())
	case LocalChange:
		return fmt.Sprintf("local change: %v", c.LocalPath())
	case LocalDelete:
		return fmt.Sprintf("local delete: %v", c.LocalPath())
	case RemoteAdd:
		return fmt.Sprintf("remote add: %v", c.RemotePath())
	case RemoteChange:
		return fmt.Sprintf("remote change: %v", c.RemotePath())
	case RemoteDelete:
		return fmt.Sprintf("remote delete: %v", c.RemotePath())
	default:
		return fmt.Sprintf("%#v", c)
	}
}

// LocalPath returns the relative local path of the change
func (c *Change) LocalPath() string {
	if c.cacheEntry != nil && c.cacheEntry.LocalPath() != c.localPath {
		panic("Cache entry and change path local paths do not match")
	}

	return c.localPath
}

// RemotePath returns the relative remote path of the change
func (c *Change) RemotePath() string {
	if c.cacheEntry != nil && c.cacheEntry.RemotePath() != c.remotePath {
		panic("Cache entry and change path remote paths do not match")
	}

	return c.remotePath
}

// ChangeQueueManager establishes change processing workers and farms work out
// to them as needed.
func ChangeQueueManager(newChanges chan *Change, completedChanges chan *Change, errors chan error, die chan bool) {
	// Need to keep track of which syncs have dequeue managers already running
	childDie := make(chan bool)
	managedSyncs := make([]*SyncInfo, 0, 5)

	// Establish workers
	var wg sync.WaitGroup
	defer func() {
		log.Println("Signalling all processing to end")
		close(childDie)
		for _, s := range managedSyncs {
			close(s.changesReady)
		}

		log.Println("Waiting for all processors to end")
		wg.Wait()
		log.Println("All processors ended")
		log.Println("Change queue manager quitting")
	}()
	log.Println("Change queue manager started")

	todo := make(chan *Change)
	failed := make(chan *Change)
	completed := make(chan *Change)

	// TODO could launch more
	wg.Add(1)
	go func() {
		changeProcessor(todo, completed, failed, errors, childDie)
		wg.Done()
	}()

	for {
		// Any time we receive a new change or complete/fail processing on a change,
		// put a new item from that sync into the processing channel
		select {
		case change := <-newChanges:
			if err := queueChange(change); err != nil {
				errors <- err
				break
			}

			// Ensure the dequeue manager is running for this sync
			found := false
			for _, v := range managedSyncs {
				// We are relying on this being the EXACT same instance
				if v == change.sync {
					found = true
					break
				}
			}
			if !found {
				managedSyncs = append(managedSyncs, change.sync)
				wg.Add(1)
				go func() {
					changePusher(change.sync, todo, errors, childDie)
					wg.Done()
				}()
			}

			change.sync.ProcessChange()

		case change := <-failed:
			// For whatever reason, this change failed ot go through. We'll try again
			// TODO announce error in some way
			log.Printf("Change %v failed, will try again", change)
			wg.Add(1)
			go func() {
				select {
				case newChanges <- change:
					change.sync.ProcessChange()
				case <-childDie:
				}
				wg.Done()
			}()

		case change := <-completed:
			// Drop change from database
			_, err := change.sync.db.Exec("DELETE FROM change_queue WHERE id=?", change.id)
			if err != nil {
				errors <- &ErrProcessor{"Error removing change from database", err}
			}

			completedChanges <- change
			change.sync.ProcessChange()

		case <-die:
			return
		}
	}
}

// Every sync should be given a single change pusher. When the sync's changesReady channel is
// hit, the database is checked for pending changes. Also periodically checks for changes.
// Closing the input channel OR the die channel will end the pusher.
func changePusher(sync *SyncInfo, processingChannel chan *Change, errors chan error, die chan bool) {
	defer func() {
		log.Printf("Stopped queue watcher for %v\n", sync.LocalBase())
	}()
	log.Printf("Starting queue watcher for %v\n", sync.LocalBase())

	tick := time.Tick(30 * time.Second)
	for {
		select {
		case <-tick:
			// Actual push occurs below
		case _, ok := <-sync.changesReady:
			if !ok {
				return
			}

		case <-die:
			return
		}

		pushNextChange(sync, processingChannel, errors, die)
	}
}

// processNextChange pushes the oldest change in the queue to the given processing channel
func pushNextChange(sync *SyncInfo, processingChannel chan *Change, errors chan error, die chan bool) {
	change := new(Change)
	change.sync = sync

	row := change.sync.db.QueryRow("SELECT id, change_type, rel_local_path, rel_remote_path FROM change_queue WHERE processing=0 ORDER BY time_added ASC LIMIT 1")
	err := row.Scan(&change.id,
		&change.changeType,
		&change.localPath,
		&change.remotePath)
	if err == sql.ErrNoRows {
		// Nothing to do
		return
	}
	if err != nil {
		// Some issue occurred and we COULDN'T get any rows because of an error
		errors <- &ErrProcessor{"Unable to retrieve next change", err}
		return
	}

	// Fill as much as we can
	if change.LocalPath() != "" {
		change.cacheEntry, err = GetCacheEntryViaLocal(change.sync, change.LocalPath())
		if err != nil && err != sql.ErrNoRows {
			errors <- err
		}
	}
	if change.cacheEntry == nil && change.remotePath != "" {
		change.cacheEntry, err = GetCacheEntryViaRemote(change.sync, change.remotePath)
		if err != nil && err != sql.ErrNoRows {
			errors <- err
		}
	}

	if change.cacheEntry != nil {
		change.localPath = change.cacheEntry.LocalPath()
		change.remotePath = change.cacheEntry.RemotePath()
	}

	// Try to push for a bit, then give up
	timeout := time.After(5 * time.Second)
	select {
	case processingChannel <- change:
		_, err = change.sync.db.Exec("UPDATE change_queue SET processing=1 WHERE id=?", change.id)
		if err != nil {
			errors <- &ErrProcessor{"Unable to mark change as in-progress", err}
		}

	case <-timeout:
		// We'll try later

	case <-die:
		// Well, not gonna do this today... we're quitting
	}
}

func queueChange(change *Change) error {
	// Should we ignore this change?
	if ignore, err := shouldIgnore(change); err != nil || ignore {
		if err != nil {
			return err
		}
		log.Println("Ignored", change)
		return nil
	}

	// TODO conflict check/resolution. Perhaps return the corrected change rather
	// than a bool?
	if drop, err := conflictExists(change); err != nil || drop {
		// TODO conflict channel?
		if err != nil {
			return err
		}
		log.Println("Conflict, skipping", change)
		return nil
	}

	_, err := change.sync.db.Exec(`INSERT INTO change_queue
																			(time_added, change_type, rel_local_path, rel_remote_path)
																			VALUES
																			(datetime('now'), ?, ?, ?)`,
		change.changeType, change.LocalPath(), change.RemotePath())
	if err != nil {
		return &ErrProcessor{"Unable to put change into database", err}
	}

	return nil
}

func changeProcessor(incoming chan *Change, completed chan *Change, failed chan *Change, errors chan error, die chan bool) {
	defer func() {
		log.Println("Change processor quitting")
	}()
	log.Println("Change processor starting")

	for {
		select {
		case change := <-incoming:
			// Extract/encrypt as necessary and update entry in db
			switch change.changeType {
			case LocalAdd:
				fallthrough
			case LocalChange:
				if change.cacheEntry == nil {
					// New entry, so generate a remote path filename
					remotePath, err := GetFreeRemotePath(change.sync)
					if err != nil {
						errors <- &ErrProcessor{"Unable to get new free remote file name", err}
						failed <- change
						break
					}

					change.remotePath = remotePath
				} else {
					// Current entry, so open the file and compare to current data
					prot, err := OpenProtectedFile(change.sync, change.RemotePath())
					if err != nil {
						// If we couldn't open it, we should fail because it might mean an encryption error (user has incorrect keys)
						errors <- &ErrProcessor{"Unable to open existing remote file", err}
						failed <- change
						break
					}
					change.protectedFile = prot
				}

				// Create/update remote
				ignoreID, err := addIgnore(change.sync, false, change.RemotePath())
				if err != nil {
					errors <- err
					failed <- change
					break
				}

				if change.protectedFile == nil {
					change.protectedFile, err = CreateProtectedFile(change.sync, change.RemotePath(), change.LocalPath())
				} else {
					// TODO we should check if hashes match up and avoid extra work here if possible
					err = change.protectedFile.Write()
				}
				ignoreErr := removeIgnore(change.sync, ignoreID)

				if err != nil {
					errors <- &ErrProcessor{"Unable to write to protected file", err}
					failed <- change
					break
				}
				if ignoreErr != nil {
					errors <- &ErrProcessor{"Failed to remove ignore", err}
				}

				// Update cache
				if err := updateCache(change); err != nil {
					errors <- &ErrProcessor{"Unable to update cache after local add/change", err}
				}

				// Regardless of issues updating our cache, we did the important part
				completed <- change

			case LocalDelete:
				// Delete the cache entry and remote file
				if change.cacheEntry == nil {
					// Report this as completed because we may have just not created the entry yet
					// IE, maybe it was a short-lived temp file
					errors <- &ErrProcessor{fmt.Sprintf("Local file %v deleted, but no remote found", change.LocalPath()), nil}
					completed <- change
					break
				}

				ignoreID, err := addIgnore(change.sync, false, change.RemotePath())
				if err != nil {
					errors <- err
				}

				err = removeIgnoreAfterHit(change.sync, ignoreID)
				if err != nil {
					errors <- &ErrProcessor{"Failed to remove ignore", err}
				}

				if err = os.Remove(change.cacheEntry.AbsRemotePath()); err != nil {
					errors <- &ErrProcessor{fmt.Sprintf("Removal of remote file %v failed", change.cacheEntry.AbsRemotePath()), err}
					failed <- change
					break
				}

				if err := change.cacheEntry.Delete(); err != nil {
					errors <- &ErrProcessor{"Failed to remove cache entry", err}
				} else {
					change.cacheEntry = nil
				}

				completed <- change

			case RemoteAdd:
				fallthrough
			case RemoteChange:
				// Extract new contents
				// TODO add accessor function to Change and have them validate things like change.remotePath == change.cacheEntry.remotePath
				prot, err := OpenProtectedFile(change.sync, change.RemotePath())
				if err != nil {
					errors <- &ErrProcessor{"Unable to open protected file", err}
					failed <- change
					break
				}

				change.protectedFile = prot
				change.localPath = prot.LocalPath()

				ignoreID, err := addIgnore(change.sync, true, change.LocalPath())
				if err != nil {
					errors <- err
					failed <- change
					break
				}

				err = prot.ExtractContents()
				ignoreErr := removeIgnore(change.sync, ignoreID)

				if err != nil {
					errors <- &ErrProcessor{"Unable to write local file", err}
					failed <- change
					break
				}
				if ignoreErr != nil {
					errors <- &ErrProcessor{"Failed to remove ignore", err}
				}

				// Update file cache
				if err = updateCache(change); err != nil {
					errors <- &ErrProcessor{"Failed to update file cache", err}
				}

				// Regardless of issues updating our cache, we did extract the file correctly
				completed <- change

			case RemoteDelete:
				// Delete the cache entry and local file
				if change.cacheEntry == nil {
					// Report this as completed because we may have just not created the entry yet
					// IE, maybe it was a short-lived temp file
					errors <- &ErrProcessor{fmt.Sprintf("Remote file %v deleted, but no local found", change.RemotePath()), nil}
					completed <- change
					break
				}

				ignoreID, err := addIgnore(change.sync, true, change.LocalPath())
				if err != nil {
					errors <- err
				}
				if err := removeIgnoreAfterHit(change.sync, ignoreID); err != nil {
					errors <- &ErrProcessor{"Failed to remove ignore", err}
				}

				if err := os.Remove(change.cacheEntry.AbsLocalPath()); err != nil {
					errors <- &ErrProcessor{fmt.Sprintf("Removal of local file %v failed", change.cacheEntry.AbsRemotePath()), err}
					failed <- change
					break
				}

				if err := change.cacheEntry.Delete(); err != nil {
					errors <- &ErrProcessor{"Failed to remove cache change.cacheEntry", err}
				}

				completed <- change

			default:
				errors <- &ErrProcessor{fmt.Sprintf("Change processor does not understand change type %v", change.changeType), nil}
				failed <- change
			}

		case <-die:
			return
		}
	}
}

func updateCache(change *Change) error {
	if change.cacheEntry == nil {
		// No cache entry yet, so we need to get it started
		change.cacheEntry = NewCacheEntry(change.sync,
			change.RemotePath(),
			change.LocalPath(),
			nil, // Going to set these just below (hashes)
			nil)
	}

	// Update file cache
	change.cacheEntry.SetLocalHash(change.protectedFile.ContentHash())
	rHash, err := change.protectedFile.RemoteHash()
	if err != nil {
		// Report error, but try to carry on. Maybe we'll at least get the local
		// hash updated
		return &ErrProcessor{"Unable to obtain remote hash from file", err}
	}

	change.cacheEntry.SetRemoteHash(rHash)
	if err = change.cacheEntry.Save(); err != nil {
		return &ErrProcessor{"Failed to update file cache", err}
	}

	return nil
}

func addIgnore(sync *SyncInfo, isLocal bool, relPath string) (id int64, err error) {
	log.Println("Adding ignore on", relPath)
	res, err := sync.db.Exec(`INSERT INTO temp_ignores
												(expires, is_local, rel_path)
												VALUES
												(datetime("now", "+1 minute"), ?, ?)`, isLocal, relPath)
	if err != nil {
		return 0, &ErrProcessor{"Unable to add ignore", err}
	}

	return res.LastInsertId()
}

// removeIgnoreAfterHit marks the given ignore line to be remove AFTER it is seen once
func removeIgnoreAfterHit(sync *SyncInfo, id int64) error {
	_, err := sync.db.Exec("UPDATE temp_ignores SET ignore_once=1 WHERE id=?", id)
	if err != nil {
		return &ErrProcessor{"Unable to set ignore to be removed next hit", err}
	}

	return nil
}

func removeIgnore(sync *SyncInfo, id int64) error {
	log.Println("Removing ignore", id)
	_, err := sync.db.Exec("DELETE FROM temp_ignores WHERE id=?", id)
	if err != nil {
		return &ErrProcessor{"Failed to remove ignore", err}
	}

	return nil
}

func shouldIgnore(change *Change) (bool, error) {
	var isLocal bool
	var path string
	switch change.changeType {
	case LocalAdd:
		fallthrough
	case LocalChange:
		fallthrough
	case LocalDelete:
		isLocal = true
		path = change.LocalPath()
	case RemoteAdd:
		fallthrough
	case RemoteChange:
		fallthrough
	case RemoteDelete:
		isLocal = false
		path = change.RemotePath()
	default:
		return false, &ErrProcessor{fmt.Sprintf("Unexpected change type %v in shouldIgnore", change.changeType), nil}
	}

	rows, err := change.sync.db.Query(`SELECT id, ignore_once
																			FROM temp_ignores
																			WHERE expires > datetime('now') AND is_local=? AND rel_path=?
																			LIMIT 1`,
		isLocal, path)
	if err == sql.ErrNoRows {
		// TODO I'm not sure this case ever happens... I believe rows may
		// always return and just be an empty result set
		return false, nil
	} else if err != nil {
		return false, &ErrProcessor{"Unable to check ignore list", err}
	}
	defer rows.Close()

	if rows.Next() {
		// We must have found rows that match
		// Should we remove the hit we found?
		var id int64
		var ignoreOnce bool
		err = rows.Scan(&id, &ignoreOnce)
		if err != nil {
			log.Println("Error checking remove once status:", err)
		}

		if ignoreOnce {
			rows.Close()
			if err = removeIgnore(change.sync, id); err != nil {
				log.Println("Error removing ignore:", err)
			}
		}

		return true, nil
	}

	return false, rows.Err()
}

func conflictExists(change *Change) (bool, error) {
	rows, err := change.sync.db.Query(`SELECT id, change_type, rel_local_path, rel_remote_path
																						FROM change_queue
																						WHERE rel_local_path=? OR rel_remote_path=?`,
		change.LocalPath(),
		change.RemotePath())
	if err == sql.ErrNoRows {
		// TODO again, not sure if this can ever happen
		return false, nil
	} else if err != nil {
		return true, &ErrProcessor{"Unable to get conflicts", err}
	}
	defer rows.Close()

	// Check each possible conflict and decide best resolution
	for rows.Next() {
		var conflict Change
		if err = rows.Scan(&conflict.id, &conflict.changeType, &conflict.localPath, &conflict.remotePath); err != nil {
			return true, &ErrProcessor{"Unable to scan possible conflict", err}
		}

		// TODO all conflicts and manage best resolution
		// For now, skip the new change
		return true, nil
	}
	if rows.Err(); err != nil {
		return true, &ErrProcessor{"Unable to check conflicts", err}
	}

	return false, nil
}

// PrepareChangeQueue readies the given sync for use with the change queue manager, including establishing
// the database schema and ensuring the queue is currently empty.
func prepareChangeQueue(sync *SyncInfo) error {
	schema := `
	  CREATE TABLE IF NOT EXISTS change_queue (id INTEGER PRIMARY KEY,
				                                     time_added DATETIME DEFAULT current_timestamp,
				                                     change_type INTEGER NOT NULL,
				                                     rel_local_path TEXT DEFAULT NULL,
				                                     rel_remote_path TEXT DEFAULT NULL,
				                                     processing BOOLEAN DEFAULT 0);
	  CREATE INDEX IF NOT EXISTS change_lp ON change_queue (rel_local_path);
	  CREATE INDEX IF NOT EXISTS change_rp ON change_queue (rel_remote_path);
	  CREATE INDEX IF NOT EXISTS change_time ON change_queue (time_added);

	  CREATE TABLE IF NOT EXISTS temp_ignores (id INTEGER PRIMARY KEY,
											                       expires INT8 NOT NULL,
                                             ignore_once BOOLEAN NOT NULL DEFAULT 0,
										                         is_local BOOLEAN NOT NULL,
										                         rel_path TEXT NOT NULL);

	  DELETE FROM change_queue;
	  DELETE FROM temp_ignores;
	`

	_, err := sync.db.Exec(schema)
	return err
}

// cleanChangeQueue removes ignores it knows are safe to delete. It does not wipe the
// change queue because that may still be current, if monitoring is active.
func cleanChangeQueue(sync *SyncInfo) error {
	_, err := sync.db.Exec("DELETE FROM temp_ignores WHERE expires < datetime('now')")
	if err != nil {
		return &ErrProcessor{"Unable to empty ignores", err}
	}

	return nil
}
