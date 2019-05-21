package main

import (
	"bytes"
	"github.com/buger/jsonparser"
	"io"
	"os"
	"strings"
)

var fileData = make([]byte, 0, 10000000)
var entityData = make([]byte, 0, 500)
var dataStartIndex int

func ResetFile(filepath string) error {
	fileData = fileData[:0]
	entityData = entityData[:0]

	file, err := os.Open(filepath)

	if err != nil {
		return err
	}

	chunk := make([]byte, 35768)

	for {
		countBytes, err := file.Read(chunk)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		fileData = append(fileData, chunk[:countBytes]...)
	}

	if strings.Contains(filepath, "user") {
		dataStartIndex = 11
	} else if strings.Contains(filepath, "visit") {
		dataStartIndex = 12
	} else if strings.Contains(filepath, "location") {
		dataStartIndex = 15
	}

	return nil
}

func ParseEntity() bool {
	if len(fileData[dataStartIndex:]) == 0 {
		return false
	}

	entityEndIndex := bytes.IndexByte(fileData[dataStartIndex:], '}')

	entityData = fileData[dataStartIndex : dataStartIndex+entityEndIndex+1]

	if len(fileData[dataStartIndex:]) == entityEndIndex+1 {
		dataStartIndex += entityEndIndex + 1
	} else {
		dataStartIndex += entityEndIndex + 3
	}

	return true
}

func GetIntValue(fieldName string) int {
	value, _ := jsonparser.GetInt(entityData, fieldName)

	return int(value)
}

func GetStringValue(fieldName string) string {
	value, _ := jsonparser.GetString(entityData, fieldName)

	return value
}
