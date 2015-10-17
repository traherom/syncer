package main

import "database/sql"

// CacheEntry is an copy of the information from the backing database in Sync
type CacheEntry struct {
	LocalPath  string    // Relative path of the unencrypted version of this file
	RemotePath string    // Relative path of the encrypted version of this file
	RemoteHash string    // HMAC of the encrypted version of this file, as pulled directly from the end
	LocalHash  string    // Hash of the local version of this file
	Sync       *SyncInfo // Sync this entry belongs to
	id         int       // Id of this entry in the database. -1 if not yet inserted
}

// GetCacheEntryViaLocal returns the file cache entry located via the relative local path given
func GetCacheEntryViaLocal(s *SyncInfo, local string) (entry CacheEntry, err error) {
	row := s.db.QueryRow("SELECT id, rel_remote_path, rel_local_path, remote_hash, local_hash FROM cache WHERE rel_local_path=? LIMIT 1", local)
	return scanToEntry(row)
}

// GetCacheEntryViaRemote returns the file cache entry located via the relative local path given
func GetCacheEntryViaRemote(s *SyncInfo, remote string) (entry CacheEntry, err error) {
	row := s.db.QueryRow("SELECT id, rel_remote_path, rel_local_path, remote_hash, local_hash FROM cache WHERE rel_remote_path=? LIMIT 1", remote)
	return scanToEntry(row)
}

func scanToEntry(row *sql.Row) (entry CacheEntry, err error) {
	err = row.Scan(&entry.id, &entry.RemotePath, &entry.LocalPath, &entry.RemoteHash, &entry.LocalHash)
	return
}
