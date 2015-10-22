package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/traherom/gocrypt"
	"github.com/traherom/gocrypt/aes"
	"github.com/traherom/gocrypt/hash"
	"github.com/traherom/memstream"
)

// A Header contains the header of an encrypted file and the metadata needed to
// work with that file
type Header struct {
	// Hash (not HMAC) of the unencrypted version of the protected contents.
	// That is, this is the hash of the protected file if it were decrypted.
	contentHash gocrypt.Hash
	localPath   string            // Path, relative to sync.LocalBase, of this file's unencrypted version
	remotePath  string            // Path, relative to sync.RemoteBase, of the protected file
	contentKeys *gocrypt.KeyCombo // Keys used for the encrypted contents of this file
	headerLen   int               // Length of header. If you open the file and seek to HeaderLen, you will be at the start of the content

	sync *SyncInfo // Associated Sync this protected file belongs too
}

// LocalPath returns the path of the unprotected, decrypted version of the file
func (h *Header) LocalPath() string {
	return h.localPath
}

// RemotePath returns the path of the protected file
func (h *Header) RemotePath() string {
	return h.remotePath
}

// AbsLocalPath returns the local path in absolute form based on the associated sync
func (h *Header) AbsLocalPath() string {
	return filepath.Join(h.sync.LocalBase(), h.localPath)
}

// AbsRemotePath returns the protected file path in absolute form, based on the
// associated sync.
func (h *Header) AbsRemotePath() string {
	return filepath.Join(h.sync.RemoteBase(), h.remotePath)
}

// Sync returns the sync associated with this file
func (h *Header) Sync() *SyncInfo {
	return h.sync
}

// ContentHash returns the hash of the unencrypted contents of this file (ie,
// after extraction the file will have this hash).
func (h *Header) ContentHash() gocrypt.Hash {
	return h.contentHash
}

// RemoteHash returns the hash of the full protected file. This is an expensive
// operation, as the value is not chached.
func (h *Header) RemoteHash() (gocrypt.Hash, error) {
	f, err := os.Open(h.AbsRemotePath())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return hash.Sha256(f)
}

// CreateProtectedFile takes the given local file and turns it into a protected file
// at the given remote path. All paths should be relative to metadata's
// respective base paths.
func CreateProtectedFile(sync *SyncInfo, protFilePath string, localFilePath string) (*Header, error) {
	header := new(Header)
	header.sync = sync
	header.localPath = localFilePath
	header.remotePath = protFilePath

	err := header.Write()
	return header, err
}

// OpenProtectedFile reads the header of the given protected file and returns the data
// needed to work further with that file.
func OpenProtectedFile(sync *SyncInfo, protFilePath string) (*Header, error) {
	f, err := os.Open(filepath.Join(sync.RemoteBase(), protFilePath))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Decrypt header
	headBytes := memstream.New()
	keys := sync.Keys()
	encHeadLen, _, err := aes.Decrypt(f, headBytes, &keys)
	if err != nil {
		return nil, err
	}

	header := new(Header)
	header.sync = sync
	header.headerLen = int(encHeadLen)
	header.remotePath = protFilePath
	header.contentKeys = new(gocrypt.KeyCombo)

	// Interpret
	headBytes.Rewind()
	buf := bufio.NewReader(headBytes)

	version, err := buf.ReadByte()
	if err != nil {
		return nil, &ErrProtectedFile{"Unable to read header version", err}
	}

	switch version {
	case 1:
		// Format:
		// 1 - version (currently at version 1)
		// 16 - crypto key for contents
		// 16 - auth key
		// 32 - hash of DECRYPTED contents
		// 8 - length of Path
		// <Variable> - local relative path
		header.contentKeys.CryptoKey, err = readKey(buf)
		if err != nil {
			return nil, &ErrProtectedFile{"Unable to read crypto key", err}
		}

		header.contentKeys.AuthKey, err = readKey(buf)
		if err != nil {
			return nil, &ErrProtectedFile{"Unable to read auth key", err}
		}

		header.contentHash, err = readFixedSize(buf, hash.Sha256Size)
		if err != nil {
			return nil, &ErrProtectedFile{"Unable to read content hash", err}
		}

		lenBytes, err := readFixedSize(buf, 8)
		pathLen := gocrypt.BytesToUint64(lenBytes)
		if err != nil {
			return nil, &ErrProtectedFile{"Unable to read path length", err}
		}

		pathBytes, err := readFixedSize(buf, int(pathLen))
		header.localPath = string(pathBytes)
		if err != nil {
			return nil, &ErrProtectedFile{"Unable to read path", err}
		}

	default:
		return nil, &ErrProtectedFile{fmt.Sprintf("Version %v of header is not recognized", version), nil}
	}

	return header, nil
}

// Write rebuilds the protected file on disk, ensuring the contents and header
// are current with the local file
func (h *Header) Write() error {
	// Get data from local file for header
	localFile, err := os.Open(h.AbsLocalPath())
	if err != nil {
		return &ErrProtectedFile{"Unable to open local file", err}
	}
	defer localFile.Close()

	h.contentHash, err = hash.Sha256(localFile)
	if err != nil {
		return &ErrProtectedFile{"Unable to get local file hash", err}
	}

	// Keys
	h.contentKeys, err = aes.NewKeyCombo()
	if err != nil {
		return &ErrProtectedFile{"Failed to generate keys for file", err}
	}

	// Write header to temp file
	headerBytes := memstream.NewCapacity(1024)
	writer := bufio.NewWriter(headerBytes)
	err = writer.WriteByte(1) // Version
	if err != nil {
		return &ErrProtectedFile{"Unable to write version", err}
	}

	_, err = writer.Write(h.contentKeys.CryptoKey)
	if err != nil {
		return &ErrProtectedFile{"Unable to write crypto key", err}
	}
	_, err = writer.Write(h.contentKeys.AuthKey)
	if err != nil {
		return &ErrProtectedFile{"Unable to write auth key", err}
	}
	_, err = writer.Write(h.contentHash)
	if err != nil {
		return &ErrProtectedFile{"Unable to write hash", err}
	}

	localLenBytes := gocrypt.Uint64ToBytes(uint64(len(h.LocalPath())))
	_, err = writer.Write(localLenBytes)
	if err != nil {
		return &ErrProtectedFile{"Unable to write path length", err}
	}

	_, err = writer.Write([]byte(h.LocalPath()))
	if err != nil {
		return &ErrProtectedFile{"Unable to write local path", err}
	}

	if err = writer.Flush(); err != nil {
		return &ErrProtectedFile{"Flushing header failed", err}
	}

	// Encrypt and write header to temp file
	protFile, err := ioutil.TempFile("", "syncerprotfile")
	if err != nil {
		return &ErrProtectedFile{"Unable to create temp protected file", err}
	}
	defer func() {
		protFile.Close()
		os.Remove(protFile.Name())
	}()

	headerBytes.Rewind()
	metakeys := h.sync.Keys()
	_, _, err = aes.Encrypt(headerBytes, protFile, &metakeys)
	if err != nil {
		return &ErrProtectedFile{"Unable to encrypt header", err}
	}

	// Update length of header. Not in file, just used internally for extracting
	headLen, err := protFile.Seek(0, 1)
	h.headerLen = int(headLen)
	if err != nil {
		return &ErrProtectedFile{"Unable to determine length of header", err}
	}

	// Encrypt and append contents
	_, err = localFile.Seek(0, 0)
	if err != nil {
		return &ErrProtectedFile{"Unable to return to beginning of local file", err}
	}

	_, _, err = aes.Encrypt(localFile, protFile, h.contentKeys)
	if err != nil {
		return &ErrProtectedFile{"Unable to encrypt contents", err}
	}

	// Move file to final location
	protFile.Close()
	err = moveFile(protFile.Name(), h.AbsRemotePath())
	if err != nil {
		return &ErrProtectedFile{"Failed to move final protected file", err}
	}

	return nil
}

// ExtractContents decrypts the contents of the protected file onto the local path
func (h *Header) ExtractContents() error {
	// Skip to contents
	protFile, err := os.Open(h.AbsRemotePath())
	if err != nil {
		return &ErrProtectedFile{"Unable to open protected file", err}
	}
	defer protFile.Close()

	_, err = protFile.Seek(int64(h.headerLen), 0)
	if err != nil {
		return &ErrProtectedFile{"Unable to skip to contents", err}
	}

	// Decrypt to temp
	tempFile, err := ioutil.TempFile("", "syncerdecryptedcontents")
	if err != nil {
		return &ErrProtectedFile{"Unable to create temp file", err}
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	_, _, err = aes.Decrypt(protFile, tempFile, h.contentKeys)
	if err != nil {
		return &ErrProtectedFile{"Error during decryption", err}
	}

	// Move to final location
	tempFile.Close()
	err = moveFile(tempFile.Name(), h.AbsLocalPath())
	if err != nil {
		return &ErrProtectedFile{"Failed to move decrypted file to local", err}
	}

	return nil
}

func readKey(buf *bufio.Reader) (gocrypt.Key, error) {
	key, err := readFixedSize(buf, aes.KeyLength)
	return key, err
}

func readFixedSize(buf *bufio.Reader, desiredCount int) ([]byte, error) {
	b := make([]byte, desiredCount)
	n, err := buf.Read(b)
	if err != nil {
		return nil, &ErrProtectedFile{"Unable to read key", err}
	}
	if n != desiredCount {
		return nil, &ErrProtectedFile{
			fmt.Sprintf("Did not read entire key (got %v of %v)", n, desiredCount),
			nil}
	}

	return b, nil
}

// ErrProtectedFile represents possible errors from a protected file
type ErrProtectedFile struct {
	Msg   string
	Inner error
}

func (e *ErrProtectedFile) Error() string {
	if e.Inner == nil {
		return fmt.Sprintf("protected file: %v", e.Msg)
	}

	return fmt.Sprintf("protected file: %v: %v", e.Msg, e.Inner)
}

// moveFile attempts to move the given file via an os.Rename(). If that
// failes, a full move with a copy of all bytes is performed, then the original
// file is deleted.
func moveFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err != nil {
		// Attempt copy-then-delete
		if err = copyFileContents(src, dst); err != nil {
			return err
		}

		if err = os.Remove(src); err != nil {
			return err
		}
	}

	return nil
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return
	}

	err = out.Sync()
	return
}
