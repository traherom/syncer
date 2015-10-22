package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
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
	LocalPath  string      // Path to changed file relative to sync.LocalBase()
	RemotePath string      // Path to changed file relative to sync.RemoteBase()
	ChangeType ChangeType  // Type of change
	CacheEntry *CacheEntry // If this change involves previously known files, this may be populated
	Sync       *SyncInfo   // Sync this change is a part of
	id         int         // Internal database id of this change
}

// ChangeQueueManager establishes change processing workers and farms work out
// to them as needed.
func ChangeQueueManager(newChanges chan *Change, completedChanges chan *Change, errors chan error, die chan bool) {
	// Establish workers
	var wg sync.WaitGroup
	defer func() {
		fmt.Println("Waiting for all change precessors to end")
		wg.Wait()
		fmt.Println("All processors ended")
	}()

	todo := make(chan *Change)
	failed := make(chan *Change)
	completed := make(chan *Change)
	//	for i := 0; i < runtime.NumCPU(); i++ {
	for i := 0; i < 1; i++ {
		fmt.Printf("Starting change processor %v\n", i+1)

		wg.Add(1)
		go func() {
			changeProcessor(todo, failed, completed, errors, die)
			wg.Done()
		}()
	}

	// Any time we receive a new change or complete/fail processing on a change,
	// put a new item from that sync into the processing channel
	pushOldestToProcessing := func(sync *SyncInfo) {
		change := new(Change)
		change.Sync = sync

		row := sync.db.QueryRow("SELECT id, change_type, rel_local_path, rel_remote_path FROM change_queue WHERE processing=0 ORDER BY time_added ASC LIMIT 1")
		err := row.Scan(&change.id, &change.ChangeType, &change.LocalPath, &change.RemotePath)
		if err == sql.ErrNoRows {
			// Nothing to do
			return
		}
		if err != nil {
			// Some issue occurred and we COULDN'T get any rows because of an error
			errors <- &ErrProcessor{"Unable to retrieve next change", err}
			return
		}

		// If we aren't able to push the change to processing (everyone is busy),
		// skip doing so. We'll push when one of them returns a change as completed.
		select {
		case todo <- change:
			_, err = sync.db.Exec("UPDATE change_queue SET processing=1 WHERE id=?", change.id)
			if err != nil {
				errors <- &ErrProcessor{"Unable to mark change as in-progress", err}
				return
			}

		default:
			// Do nothing, will try again later
		}
	}

	for {
		// New change?
		select {
		case change := <-newChanges:
			// Should ignore?
			if ignore, err := shouldIgnore(change); err != nil || ignore {
				if err != nil {
					errors <- err
				}
				fmt.Println("Ignored", change)
				break
			}

			// Check for conflict
			if conflict, err := conflictExists(change); err != nil || conflict {
				// TODO conflict channel?
				if err != nil {
					errors <- err
				}
				fmt.Println("Conflict, skipping", change)
				break
			}

			// Put in database
			res, err := change.Sync.db.Exec(`INSERT INTO change_queue
																					(time_added, change_type, rel_local_path, rel_remote_path)
																				VALUES
																					(datetime('now'), ?, ?, ?)`,
				change.ChangeType, change.LocalPath, change.RemotePath)
			if err != nil {
				errors <- &ErrProcessor{"Unable to put change into database", err}
			}

			id, err := res.LastInsertId()
			if err != nil {
				errors <- &ErrProcessor{"Unable to get ID of newly inserted id", err}
			}

			change.id = int(id)

			// Put oldest change (may not be the one that just came in) on processing channel
			pushOldestToProcessing(change.Sync)

		case change := <-failed:
			// For whatever reason, this change failed ot go through. We'll try again
			// TODO announce error in some way
			newChanges <- change
			pushOldestToProcessing(change.Sync)

		case change := <-completed:
			// Drop change from database
			_, err := change.Sync.db.Exec("DELETE FROM change_queue WHERE id=?", change.id)
			if err != nil {
				errors <- &ErrProcessor{"Error removing change from database", err}
			}

			pushOldestToProcessing(change.Sync)

			completedChanges <- change

		case <-die:
			fmt.Println("Change queue manager quitting")
			return
		}
	}
}

func changeProcessor(incoming chan *Change, failed chan *Change, completed chan *Change, errors chan error, die chan bool) {
	defer func() {
		fmt.Println("Change processor quitting")
	}()

	for {
		select {
		case change := <-incoming:
			// Extract/encrypt as necessary and update entry in db
			switch change.ChangeType {
			case LocalAdd:
				fallthrough
			case LocalChange:
				// Place/overwrite remote
				entry, err := GetCacheEntryViaLocal(change.Sync, change.LocalPath)
				if err != nil && err != sql.ErrNoRows {
					errors <- err
					break
				}

				var prot *Header
				if entry != nil {
					// Overwrite current entry
					prot, err = OpenProtectedFile(change.Sync, entry.RemotePath())
					if err != nil {
						errors <- &ErrProcessor{"Unable to open protected file", err}
						failed <- change
						break
					}

					if err = prot.Write(); err != nil {
						errors <- &ErrProcessor{"Unable to write to protected file", err}
						failed <- change
						break
					}
				} else {
					// New entry, so generate a remote path filename
					remotePath, err := GetFreeRemotePath(change.Sync)
					if err != nil {
						errors <- &ErrProcessor{"Unable to get new free remote file name", err}
						failed <- change
						break
					}

					prot, err = CreateProtectedFile(change.Sync, remotePath, change.LocalPath)
					if err != nil {
						errors <- &ErrProcessor{"Unable to update protected file", err}
						failed <- change
						break
					}

					// No cache entry yet, so we need to get it started
					entry = NewCacheEntry(prot.Sync(),
						prot.RemotePath(),
						prot.LocalPath(),
						nil, // Going to set these just belowe (hashes)
						nil)
				}

				// Update file cache
				entry.SetLocalHash(prot.ContentHash())
				rHash, err := prot.RemoteHash()
				if err != nil {
					// Report error, but try to carry on. Maybe we'll at least get the local
					// hash updated
					errors <- &ErrProcessor{"Unable to obtain remote hash from file", err}
				} else {
					entry.SetRemoteHash(rHash)
				}

				if err = entry.Save(); err != nil {
					errors <- &ErrProcessor{"Failed to update file cache", err}
				}

				// Regardless of issues updating our cache, we did extract the file correctly
				completed <- change

			case LocalDelete:
				// Delete the cache entry and remote file
				entry, err := GetCacheEntryViaLocal(change.Sync, change.LocalPath)
				if err != nil && err != sql.ErrNoRows {
					errors <- &ErrProcessor{"Unable to get cache entry", nil}
					failed <- change
					break
				}
				if entry == nil {
					// Report this as completed because we may have just not created the entry yet
					// IE, maybe it was a short-lived temp file
					errors <- &ErrProcessor{fmt.Sprintf("Local file %v deleted, but no remote found", change.LocalPath), nil}
					completed <- change
					break
				}

				if err = os.Remove(entry.AbsRemotePath()); err != nil {
					errors <- &ErrProcessor{fmt.Sprintf("Removal of remote file %v failed", entry.AbsRemotePath()), err}
				}
				if err = entry.Delete(); err != nil {
					errors <- &ErrProcessor{"Failed to remove cache entry", err}
				}

				completed <- change

			case RemoteAdd:
				fallthrough
			case RemoteChange:
				// Extract new contents
				prot, err := OpenProtectedFile(change.Sync, change.RemotePath)
				if err != nil {
					errors <- &ErrProcessor{"Unable to open protected file", err}
					failed <- change
					break
				}

				if err = prot.ExtractContents(); err != nil {
					errors <- &ErrProcessor{"Unable to write local file", err}
					failed <- change
					break
				}

				// Update/create cache entry
				entry, err := GetCacheEntryViaRemote(change.Sync, change.RemotePath)
				if err != nil && err != sql.ErrNoRows {
					errors <- err
					break
				}

				if entry == nil {
					// No cache entry yet, so we need to get it started
					entry = NewCacheEntry(prot.Sync(),
						prot.RemotePath(),
						prot.LocalPath(),
						nil, // Going to set these below (hashes)
						nil)
				}

				// Update file cache
				entry.SetLocalHash(prot.ContentHash())
				rHash, err := prot.RemoteHash()
				if err != nil {
					// Report error, but try to carry on. Maybe we'll at least get the local
					// hash updated
					errors <- &ErrProcessor{"Unable to obtain remote hash from file", err}
				} else {
					entry.SetRemoteHash(rHash)
				}

				if err = entry.Save(); err != nil {
					errors <- &ErrProcessor{"Failed to update file cache", err}
				}

				// Regardless of issues updating our cache, we did extract the file correctly
				completed <- change

			case RemoteDelete:
				// TODO almost identical to local delete
				// Delete the cache entry and local file
				entry, err := GetCacheEntryViaRemote(change.Sync, change.RemotePath)
				if err != nil && err != sql.ErrNoRows {
					errors <- &ErrProcessor{"Unable to get cache entry", nil}
					failed <- change
					break
				}
				if entry == nil {
					// Report this as completed because we may have just not created the entry yet
					// IE, maybe it was a short-lived temp file
					errors <- &ErrProcessor{fmt.Sprintf("Remote file %v deleted, but no local found", change.RemotePath), nil}
					completed <- change
					break
				}

				if err = os.Remove(entry.AbsLocalPath()); err != nil {
					errors <- &ErrProcessor{fmt.Sprintf("Removal of local file %v failed", entry.AbsRemotePath()), err}
				}
				if err = entry.Delete(); err != nil {
					errors <- &ErrProcessor{"Failed to remove cache entry", err}
				}

				completed <- change

			default:
				errors <- &ErrProcessor{fmt.Sprintf("Change processor does not understand change type %v", change.ChangeType), nil}
				failed <- change
			}

		case <-die:
			return
		}
	}
}

// PrepareChangeQueue readies the given sync for use with the change queue manager, including establishing
// the database schema and ensuring the queue is currently empty.
func PrepareChangeQueue(sync *SyncInfo) error {
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
										                         is_local BOOLEAN NOT NULL,
										                         rel_path TEXT NOT NULL);

	  DELETE FROM change_queue;
	  DELETE FROM temp_ignores;
	`

	_, err := sync.db.Exec(schema)
	return err
}

func shouldIgnore(change *Change) (bool, error) {
	var isLocal bool
	var path string
	switch change.ChangeType {
	case LocalAdd:
		fallthrough
	case LocalChange:
		fallthrough
	case LocalDelete:
		isLocal = true
		path = change.LocalPath
	case RemoteAdd:
		fallthrough
	case RemoteChange:
		fallthrough
	case RemoteDelete:
		isLocal = false
		path = change.RemotePath
	default:
		return false, &ErrProcessor{fmt.Sprintf("Unexpected change type %v in shouldIgnore", change.ChangeType), nil}
	}

	rows, err := change.Sync.db.Query(`SELECT id
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
		return true, nil
	}

	return false, rows.Err()
}

func conflictExists(change *Change) (bool, error) {
	rows, err := change.Sync.db.Query(`SELECT id, change_type, rel_local_path, rel_remote_path
																						FROM change_queue
																						WHERE rel_local_path=? OR rel_remote_path=?`,
		change.LocalPath,
		change.RemotePath)
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
		if err = rows.Scan(&conflict.id, &conflict.ChangeType, &conflict.LocalPath, &conflict.RemotePath); err != nil {
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
