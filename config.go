package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v3"
)

// AppConfig
type AppConfig struct {
	Oracle     Oracle  `yaml:"oracle"`
	Maria      Maria   `yaml:"maria"`
	Tables     []Table `yaml:"tables"`
	EucKrLog   bool    `yaml:"euckr_log"`   // ture면 로그를 euc-kr로 쓰고, 아니면, utf-8로 쓴다.
	CheckEucKr bool    `yaml:"check_euckr"` // true면 euckr 범위를 벗어나는지 체크해서 insert 하지 않고, 별도 로그로 남긴다.
	BrokenLog  string  `yaml:"broken_log"`
	DbErrLog   string  `yaml:"dberr_log"`
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
	BeforeTruncate string   `yaml:"before_truncate"`
	Ip             string   `yaml:"ip"`
	Port           int      `yaml:"port"`
	Database       string   `yaml:"database"`
	User           string   `yaml:"user"`
	Password       string   `yaml:"password"`
	FetchSize      int      `yaml:"fetch_size"`
	BeforeQuerys   []string `yaml:"before_query"`
	AfterQuerys    []string `yaml:"after_query"`
}

// Tables
type Table struct {
	SourceName     string   `yaml:"name"`        // Oracle Table name
	TargetName     string   `yaml:"target_name"` // Maria Table name
	StartIndex     int      `yaml:"start_idx"`
	EndIndex       int      `yaml:"end_idx"`
	FetchSize      int      `yaml:"fetch_size"`
	BeforeTruncate string   `yaml:"before_truncate"`
	SkipColumns    []string `yaml:"skip_columns"`
	ThreadCount    int      `yaml:"thread_count"`
	Where          string   `yaml:"where"`
}

// isSkipField fieldName이 SkipColumns 포함되어 있는지 체크한다.
func (t *Table) isSkipField(fieldName string) bool {

	for _, f := range t.SkipColumns {
		if fieldName == f {
			return true
		}
	}

	return false
}

// checkRequired 필수값을 체크하고, 기본값을 셋팅한다.
func (conf *AppConfig) checkRequired() error {

	if len(conf.BrokenLog) <= 0 {
		return errors.New("broken log filename is null")
	}

	if len(conf.DbErrLog) <= 0 {
		return errors.New("db error log filename is null")
	}

	// table default 값은 maria 설정을 따른다.
	for i, t := range conf.Tables {

		if len(conf.Tables[i].TargetName) <= 0 {
			conf.Tables[i].TargetName = conf.Tables[i].SourceName
		}

		// 테이블 명은 대문자로
		conf.Tables[i].TargetName = strings.ToUpper(conf.Tables[i].TargetName)

		if t.ThreadCount <= 0 {
			conf.Tables[i].ThreadCount = 1
		}

		if t.FetchSize <= 0 {
			conf.Tables[i].FetchSize = conf.Maria.FetchSize
		}

		if !strings.EqualFold(t.BeforeTruncate, "true") && !strings.EqualFold(t.BeforeTruncate, "false") {
			conf.Tables[i].BeforeTruncate = conf.Maria.BeforeTruncate
		}
	}

	return nil
}

func (conf *AppConfig) readConfig(fileName string) error {

	Info.Println("Read " + fileName)

	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("cannot read config file %s, ReadFile: %v", fileName, err)
	}

	err = yaml.Unmarshal(buf, conf)
	if err != nil {
		return fmt.Errorf("invaild config file %s, Unmarshal: %v", fileName, err)
	}

	return nil
}

func makePretty(conf *AppConfig) string {

	buf, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		Info.Fatalf(err.Error())
	}

	return string(buf)
}
