package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

var conf AppConfig // yamlFile에서 읽은 설정

var yamlFile string       // args 에서 읽은 .yml 파일명
var isNoWriteLogFile bool // args 에서 읽은 로그 파일로 쓸지 여부
var enableTraceLog bool   // args 에서 Trace 로그를 사용할지 여부
var enableSequential bool // table별로 순차 처리할지 여부

func usage() {
	fmt.Printf("Usage: %s -config=\"yaml file\" [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&yamlFile, "config", "config.yml", "config yaml file name(.yml)")
	flag.BoolVar(&isNoWriteLogFile, "nolog", false, "if true, NO file log is written. only stdio")
	flag.BoolVar(&enableTraceLog, "trace", false, "if true, enable trace log")
	flag.BoolVar(&enableSequential, "sequential", false, "if true, enable sequential processing by table")

	flag.Usage = usage
}

func getConfigName() string {

	if _, err := os.Stat(yamlFile); os.IsNotExist(err) {
		usage()
		Error.Printf("not exists file: %s", yamlFile)
		return "config.yml"
	}

	return yamlFile
}

func initConf() {

	// read config
	err := conf.readConfig(getConfigName())
	if err != nil {
		Info.Fatal(err)
	}

	// 필수값을 체크하고, 기본값을 셋팅한다.
	err = conf.checkRequired()
	if err != nil {
		Info.Fatal(err)
	}

	Info.Printf("%+v\n", makePretty(&conf))
}

func main() {

	showVersion()
	flag.Parse()

	// setting log
	sysLog := InitLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)

	initConf()

	brokenLog, dbErrLog := setExceptLogFile(conf.BrokenLog, conf.DbErrLog)

	// 파일들을 닫는다.
	defer func() {
		brokenLog.WriteString("commit;\n")
		dbErrLog.WriteString("commit;\n")
		brokenLog.Close()
		dbErrLog.Close()
		if sysLog != nil {
			sysLog.Close()
		}
	}()

	startTimeAll := time.Now()

	// 마이그레이션전 쿼리
	for _, query := range conf.Maria.BeforeQuerys {
		execNewQuery(query)
	}

	// 마이그레이션
	var wait sync.WaitGroup

	for _, tableInfo := range conf.Tables {

		if strings.Contains(tableInfo.SourceName, "%") {

			// src 테이블이 복수개인 경우
			// 복수개는 무조건 병렬 처리
			migrationAndReportMulti(tableInfo)

		} else {

			if enableSequential {
				// 테이블별 순차 처리
				migrationAndReport(tableInfo)
			} else {
				// 테이블별 병렬 처리
				wait.Add(1)
				go func(tableInfo Table) {
					migrationAndReport(tableInfo)
					wait.Done()
				}(tableInfo)
			}
		}
	}

	// 모든 thread가 끝나길 대기
	wait.Wait()

	// 마이그레이션후 쿼리
	for _, query := range conf.Maria.AfterQuerys {
		execNewQuery(query)
	}

	duration := time.Since(startTimeAll)
	Info.Printf("%d Tables Duration %v", len(conf.Tables), duration)

	Info.Println("End Job.")
}
