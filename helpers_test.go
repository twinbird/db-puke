package main

import (
	"bytes"
	"log"
	"os"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
)

func AssertCompareFiles(t *testing.T, got, want string) {
	got_data, err := os.ReadFile(got)
	if err != nil {
		t.Fatalf("file compare failed: %v", err)
	}

	want_data, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("file compare failed: %v", err)
	}
	ret := bytes.Equal(got_data, want_data)

	if ret == false {
		t.Errorf("output file is not equal. want:%s, got:%s", want_data, got_data)
	}
}

func RemoveTestOutputFile(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal("directory remove failed.", err)
	}
}
