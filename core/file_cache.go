package core

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/traherom/gocrypt"
)

// ErrCache is a universal error type for all sync issues
type ErrCache struct {
	msg   string // Error message
	inner error  // Inner error that caused issue
}

func (e *ErrCache) Error() string {
	if e.inner == nil {
		return fmt.Sprintf("cache: %v", e.msg)
	}

	return fmt.Sprintf("cache: %v: %v", e.msg, e.inner.Error())
}

// CacheEntry is an copy of the information from the backing database in Sync
type CacheEntry struct {
	localPath  string       // Relative path of the unencrypted version of this file
	remotePath string       // Relative path of the encrypted version of this file
	remoteHash gocrypt.Hash // Hash of the encrypted version of this file
	localHash  gocrypt.Hash // Hash of the local version of this file
	sync       *SyncInfo    // Sync this entry belongs to
	id         int          // Id of this entry in the database. 0 if not yet inserted
}

// LocalPath returns the path to the local file relative to sync.LocalBase()
func (e *CacheEntry) LocalPath() string {
	return e.localPath
}

// RemotePath returns the path to the remote file relative to sync.RemoteBase()
func (e *CacheEntry) RemotePath() string {
	return e.remotePath
}

// AbsLocalPath returns the absolute path to the referenced local, unprotected file.
func (e *CacheEntry) AbsLocalPath() string {
	return filepath.Join(e.sync.LocalBase(), e.localPath)
}

// AbsRemotePath returns the absolute path to the referenced remote (protected)
// file.
func (e *CacheEntry) AbsRemotePath() string {
	return filepath.Join(e.sync.RemoteBase(), e.remotePath)
}

// Sync returns the sync associated with this cache entry.
func (e *CacheEntry) Sync() *SyncInfo {
	return e.sync
}

// LocalHash returns the last known hash of the local file
func (e *CacheEntry) LocalHash() gocrypt.Hash {
	return e.localHash
}

// SetLocalHash updates the local hash for this cache instance in memory.
// Changes are not pushed to the database until Save() is called.
func (e *CacheEntry) SetLocalHash(newHash gocrypt.Hash) {
	e.localHash = newHash
}

// RemoteHash returns the last known hash of the remote file
func (e *CacheEntry) RemoteHash() gocrypt.Hash {
	return e.remoteHash
}

// SetRemoteHash updates the remote hash for this cache instance in memory.
// Changes are not pushed to the database until Save() is called.
func (e *CacheEntry) SetRemoteHash(newHash gocrypt.Hash) {
	e.remoteHash = newHash
}

// NewCacheEntry generates  a file cache entry ready to be inserted into the
// database. The caller must call Save() on the entry to actually save. All
// paths should be relative to sync's bases.
func NewCacheEntry(sync *SyncInfo, remotePath string, localPath string, localHash gocrypt.Hash, remoteHash gocrypt.Hash) (entry *CacheEntry) {
	entry = &CacheEntry{
		sync:       sync,
		remotePath: remotePath,
		localPath:  localPath,
		remoteHash: remoteHash,
		localHash:  localHash,
	}

	return entry
}

// Save pushes cache entry back to the database, commiting any changes made
// by holders of the entry.
func (e *CacheEntry) Save() error {
	_ = "breakpoint"
	// Assume that if we have any ID that we just need to update
	if e.id > 0 {
		res, err := e.sync.db.Exec("UPDATE cache SET remote_hash=?, local_hash=? WHERE id=?",
			[]byte(e.remoteHash),
			[]byte(e.localHash),
			e.id)
		if err != nil {
			return &ErrCache{"Unable to update cache entry", err}
		} else if cnt, err := res.RowsAffected(); cnt != 1 {
			return &ErrCache{fmt.Sprintf("Did not update one cache entry as expected (updated %v)", cnt), err}
		}
		return nil
	}

	res, err := e.sync.db.Exec("INSERT INTO cache (rel_remote_path, rel_local_path, remote_hash, local_hash) VALUES (?, ?, ?, ?)",
		e.remotePath,
		e.localPath,
		[]byte(e.remoteHash),
		[]byte(e.localHash))
	if err != nil {
		return &ErrCache{"Unable to create new cache entry", err}
	}

	id, err := res.LastInsertId()
	if err != nil {
		return &ErrCache{"Unable to retrieve new cache entry id", err}
	}

	e.id = int(id)

	return nil
}

// Delete removes the cache entry from the database.
func (e *CacheEntry) Delete() error {
	if e.id < 1 {
		return &ErrCache{"Unable to delete, row id unknown or not inserted into database", nil}
	}

	res, err := e.sync.db.Exec("DELETE FROM cache WHERE id=?", e.id)
	if err != nil {
		return &ErrCache{"Unable to delete from cache", err}
	} else if cnt, err := res.RowsAffected(); cnt != 1 {
		return &ErrCache{fmt.Sprintf("Did not delete one cache entry as expected (updated %v)", cnt), err}
	}

	return nil
}

// GetCacheEntryViaLocal returns the file cache entry located via the relative local path given
func GetCacheEntryViaLocal(s *SyncInfo, local string) (entry *CacheEntry, err error) {
	row := s.db.QueryRow("SELECT id, rel_remote_path, rel_local_path, remote_hash, local_hash FROM cache WHERE rel_local_path=? LIMIT 1", local)
	entry, err = scanToEntry(row)
	if err != nil {
		return nil, err
	}

	entry.sync = s
	return
}

// GetCacheEntryViaRemote returns the file cache entry located via the relative local path given
func GetCacheEntryViaRemote(s *SyncInfo, remote string) (entry *CacheEntry, err error) {
	row := s.db.QueryRow("SELECT id, rel_remote_path, rel_local_path, remote_hash, local_hash FROM cache WHERE rel_remote_path=? LIMIT 1", remote)
	entry, err = scanToEntry(row)
	if err != nil {
		return nil, err
	}

	entry.sync = s
	return
}

func scanToEntry(row *sql.Row) (*CacheEntry, error) {
	entry := new(CacheEntry)
	err := row.Scan(&entry.id,
		&entry.remotePath,
		&entry.localPath,
		&entry.remoteHash,
		&entry.localHash)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// GetFreeRemotePath returns a path relative to s.RemoteBase() that is currently
// available and can be used for a protected file.
func GetFreeRemotePath(s *SyncInfo) (path string, err error) {
	// Random string code from
	// http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	RandStringBytes := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return string(b)
	}

	for attempts := 0; attempts < 100; attempts++ {
		path = RandStringBytes(32+attempts) + ".synced"
		if _, err = os.Stat(filepath.Join(s.RemoteBase(), path)); os.IsNotExist(err) {
			return path, nil
		}
	}

	return "", &ErrProtectedFile{"Unable to locate a free path", err}
}