// utils/file_utils.go
package utils

import (
	"fmt"
	"path/filepath"
	"time"
)

// timeNow is a variable that holds the current time function.
// This allows it to be mocked in tests.
var timeNow = time.Now

// GenerateJSONFilePath creates the full path for the JSON file based on the current time,
// the provided base output directory, country, and page number.
func GenerateJSONFilePath(baseOutputDir, country string, page int) (string, string) {
	now := timeNow()
	yearDir := now.Format("2006")
	monthDir := now.Format("01")
	filename := fmt.Sprintf("%s_%s_page%d.json", now.Format("2006-01-02_15-04-05"), country, page)
	fullOutputDir := filepath.Join(baseOutputDir, yearDir, monthDir)
	fullJSONPath := filepath.Join(fullOutputDir, filename)

	return fullOutputDir, fullJSONPath
}