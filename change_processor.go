package main

import (
	"fmt"
	"runtime"
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
func ChangeQueueManager(newChanges chan Change, errors chan error, die chan bool) {
	// Establish workers
	var wg sync.WaitGroup
	defer func() {
		fmt.Println("Waiting for all change precessors to end")
		wg.Wait()
		fmt.Println("All processors ended")
	}()

	todo := make(chan Change)
	failed := make(chan Change)
	completed := make(chan Change)
	for i := 0; i < runtime.NumCPU(); i++ {
		fmt.Printf("Starting change processor %v\n", i+1)

		wg.Add(1)
		go func() {
			changeProcessor(todo, failed, completed, die)
			wg.Done()
		}()
	}

	// Any time we receive a new change or complete/fail processing on a change,
	// put a new item from that sync into the processing channel
	pushOldestToProcessing := func(sync *SyncInfo) {
		var change Change
		row := sync.db.QueryRow("SELECT id, change_type, rel_local_path, rel_remote_path FROM change_queue WHERE processing=0 ORDER BY time_added ASC LIMIT 1")
		err := row.Scan(&change.id, &change.ChangeType, &change.LocalPath, &change.RemotePath)
		if err != nil {
			errors <- &ErrProcessor{"Unable to retrieve next change", err}
			return
		}

		_, err = sync.db.Exec("UPDATE change_queue SET processing=1 WHERE id=?", change.id)
		if err != nil {
			errors <- &ErrProcessor{"Unable to mark change as in-progress", err}
			return
		}

		todo <- change
	}

	for {
		// New change?
		select {
		case change := <-newChanges:
			// TODO Check for conflict
			//if conflictExists(change) {

			//}

			// Put in database
			res, err := change.Sync.db.Exec("INSERT INTO change_queue (time_added, change_type, rel_local_path, rel_remote_path) VALUES (datetime('now'), ?, ?, ?)",
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
			_, err := change.Sync.db.Exec("DELETE FROM change_queue WHERE id=? LIMIT 1", change.id)
			if err != nil {
				errors <- &ErrProcessor{"Error removing change from database", err}
			}

			pushOldestToProcessing(change.Sync)

		case <-die:
			fmt.Println("Change queue manager quitting")
			return
		}
	}
}

func changeProcessor(incoming chan Change, failed chan Change, completed chan Change, die chan bool) {
	defer func() {
		fmt.Println("Change processor quitting")
	}()

	for {
		select {
		case change := <-incoming:
			// TODO extract/encrypt as necessary and update entry in db
			switch change.ChangeType {
			case LocalAdd:
				fallthrough
			case LocalChange:
				// Place/overwrite remote
				fallthrough

			case LocalDelete:
				fallthrough
			case RemoteAdd:
				fallthrough
			case RemoteChange:
				fallthrough
			case RemoteDelete:
				break
			}

			// Return to ChangeQueueManager as completed or failed, as appropriate
			//completed <- change
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
