package main

import "github.com/traherom/gocrypt"

// Hash represents a file hash
type Hash []byte

// A ProtectedFile contains the data needed to work with an encrypted
// file in the Syncer format
type ProtectedFile struct {
	// Hash (not HMAC) of the unencrypted version of the protected contents.
	// That is, this is the hash of the protected file if it were decrypted.
	contentHash Hash

	localPath   string            // Path, relative to Metadata.LocalBase, of this file's unencrypted version
	remotePath  string            // Path, relative to Metadata.RemoteBase, of the protected file
	contentKeys *gocrypt.KeyCombo // Keys used for the encrypted contents of this file

	metadata *SyncInfo
}
