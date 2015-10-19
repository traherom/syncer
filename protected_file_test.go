package main

import (
	"io/ioutil"
	"path/filepath"
	"testing"
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

	// Extract under a different name and compare
	header.localPath = "extracted.txt"
	err = header.ExtractContents()
	if err != nil {
		t.Error(err)
		return
	}

}
