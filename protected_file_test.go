package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/traherom/gocrypt"
)

func TestCreate(t *testing.T) {
	//defer cleanup()

	sync, err := CreateSync(localDir, remoteDir, mainKeys)
	if err != nil {
		t.Error(err)
		return
	}
	defer sync.Close()

	const localFilePath = "orig.txt"
	const protFilePath = "yabadabadoo.synced"

	err = ioutil.WriteFile(filepath.Join(localDir, localFilePath), []byte("test"), 0777)
	if err != nil {
		t.Error(err)
		return
	}

	header, err := CreateProtectedFile(sync, protFilePath, localFilePath)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println("Local:", header.localPath)
	fmt.Println("Remote:", header.remotePath)
	fmt.Println("Len:", header.headerLen)
	fmt.Printf("Content hash (%v bytes): %v\n", len(header.contentHash), gocrypt.BytesToB64(header.contentHash))

	// Extract under a different name and compare
	header.localPath = "extracted.txt"
	err = header.ExtractContents()
	if err != nil {
		t.Error(err)
		return
	}

}
