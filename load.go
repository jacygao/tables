package tables

import (
	"errors"
	"io/ioutil"
	"runtime"
	"strings"

	"gopkg.in/yaml.v2"
)

// Load loads config yaml file and unmarshal config data to a slice of TableInfo
func Load() ([]TableInfo, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return nil, errors.New("Failed to retrieve file path")
	}
	file = strings.TrimRight(file, "load.go")

	data, err := ioutil.ReadFile(file + "tables.yaml")
	if err != nil {
		return nil, err
	}

	tables := []TableInfo{}

	if err := yaml.Unmarshal(data, &tables); err != nil {
		return nil, err
	}

	return tables, nil
}
