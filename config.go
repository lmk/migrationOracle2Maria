package main

import (
	"encoding/json"
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v3"
)

// AppConfig
type AppConfig struct {
	Oracle Oracle  `yaml:"oracle"`
	Maria  Maria   `yaml:"maria"`
	Tables []Table `yaml:"tables"`
	//MaxThread int      `yaml:"max_thread"`
}

// Oracle
type Oracle struct {
	Ip       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// Maria
type Maria struct {
	BeforeTruncate string `yaml:"before_truncate"`
	Ip             string `yaml:"ip"`
	Port           int    `yaml:"port"`
	Database       string `yaml:"database"`
	User           string `yaml:"user"`
	Password       string `yaml:"password"`
	FetchSize      int    `yaml:"fetch_size"`
}

// Tables
type Table struct {
	Name           string   `yaml:"name"`
	FetchSize      int      `yaml:"fetch_size"`
	BeforeTruncate string   `yaml:"before_truncate"`
	SkipColumns    []string `yaml:"skip_columns"`
	ThreadCount    int      `yaml:"thread_count"`
}

func readConfig(fileName string) (*AppConfig, error) {

	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	p := &AppConfig{}
	err = yaml.Unmarshal(buf, p)
	if err != nil {
		Info.Fatalf("Unmarshal: %v", err)
	}

	// table default 값은 maria 설정을 따른다.
	for i, t := range p.Tables {
		if t.ThreadCount <= 0 {
			p.Tables[i].ThreadCount = 1
		}

		if t.FetchSize <= 0 {
			p.Tables[i].FetchSize = p.Maria.FetchSize
		}

		if !strings.EqualFold(t.BeforeTruncate, "true") && !strings.EqualFold(t.BeforeTruncate, "false") {
			p.Tables[i].BeforeTruncate = p.Maria.BeforeTruncate
		}
	}

	return p, nil
}

func makePretty(conf *AppConfig) string {

	buf, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		Info.Fatalf(err.Error())
	}

	return string(buf)
}
