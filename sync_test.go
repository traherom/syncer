package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/traherom/gocrypt"
)

const localDir = "test/local"
const remoteDir = "test/remote"

var mainKeys = &gocrypt.KeyCombo{
	gocrypt.Key{0xd3, 0x51, 0x87, 0x67, 0xad, 0xe9, 0x28, 0x33, 0x31, 0xc5, 0x26, 0x64, 0x8b, 0x79, 0xc7, 0x1f},
	gocrypt.Key{0xa7, 0x73, 0xa8, 0xff, 0x8c, 0xdf, 0x63, 0xd3, 0xcd, 0x1d, 0x0, 0x5c, 0x80, 0x1b, 0x20, 0x60},
}

// TestCreateLoadSync creates a sync, loads that same sync, and ensures the loaded
// sync is identical to the created one.
func TestCreateLoadSync(t *testing.T) {
	defer cleanup()

	created, err := CreateSync(localDir, remoteDir, mainKeys)
	if err != nil {
		t.Error(err)
		return
	}
	defer created.Close()

	loaded, err := LoadSync(localDir)
	if err != nil {
		t.Error(err)
		return
	}
	defer loaded.Close()

	// Compare created vs. loaded
	if created.LocalBase() != loaded.LocalBase() {
		t.Errorf("Local bases do not match: '%v' vs. '%v'", created.LocalBase(), loaded.LocalBase())
	}
	if created.RemoteBase() != created.RemoteBase() {
		t.Errorf("Remote bases do not match: '%v' vs. '%v'", created.RemoteBase(), loaded.RemoteBase())
	}
	if !gocrypt.IsKeyComboEqual(created.Keys(), loaded.Keys()) {
		t.Errorf("Keys do not match: %v vs. %v", created.Keys(), loaded.Keys())
	}
}

func cleanup() {
	if err := os.RemoveAll(filepath.Dir(localDir)); err != nil {
		fmt.Println("Unable to cleanup:", err)
	}
}
