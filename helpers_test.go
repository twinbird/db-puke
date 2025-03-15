package main

import (
	"bytes"
	"os"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
)

func CompareFiles(file1, file2 string) (bool, error) {
	data1, err := os.ReadFile(file1)
	if err != nil {
		return false, err
	}

	data2, err := os.ReadFile(file2)
	if err != nil {
		return false, err
	}

	return bytes.Equal(data1, data2), nil
}

func AssertCompareFiles(t *testing.T, file1, file2 string) {
	ret, err := CompareFiles(file1, file2)
	if err != nil {
		t.Errorf("file compare failed: %v", err)
	}

	if ret == false {
		t.Errorf("output file is not equal")
	}
}
