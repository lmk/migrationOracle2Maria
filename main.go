package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

var conf AppConfig // yamlFile에서 읽은 설정

var yamlFile string       // args 에서 읽은 .yml 파일명
var isNoWriteLogFile bool // args 에서 읽은 로그 파일로 쓸지 여부
var enableTraceLog bool   // args 에서 Trace 로그를 사용할지 여부

var waitGlobal sync.WaitGroup

func usage() {
	fmt.Printf("Usage: %s -config=\"yaml file\" [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func openFile(filename string, flag int) *os.File {

	logFile, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		panic(err)
	}

	return logFile
}

func init() {
	flag.StringVar(&yamlFile, "config", "config.yml", "config yaml file name(.yml)")
	flag.BoolVar(&isNoWriteLogFile, "nolog", false, "if true, NO file log is written. only stdio")
	flag.BoolVar(&enableTraceLog, "trace", false, "if true, enable trace log")

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

func main() {

	flag.Parse()

	// setting log
	InitLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)

	var sysLog *os.File
	if !isNoWriteLogFile {
		os.Mkdir("logs", 0755)
		sysLog = openFile("logs/sys_"+time.Now().Format("20060102")+".log", os.O_CREATE|os.O_APPEND|os.O_RDWR)

		multiWriter := io.MultiWriter(sysLog, os.Stdout)
		if enableTraceLog {
			Trace.SetOutput(multiWriter)
		} else {
			Trace.SetOutput(ioutil.Discard)
		}
		Info.SetOutput(multiWriter)
		Warning.SetOutput(multiWriter)

		multiWriter = io.MultiWriter(sysLog, os.Stderr)
		Error.SetOutput(multiWriter)
	}
	defer sysLog.Close()

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

	// euckr아닌 sql
	brokenLog := openFile(conf.BrokenLog, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	defer brokenLog.Close()

	// db 에러가 난 경우 sql
	dbErrLog := openFile(conf.DbErrLog, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	defer dbErrLog.Close()

	setExceptLogFile(brokenLog, dbErrLog)

	startTime := time.Now()

	// 마이그레이션전 쿼리
	for _, query := range conf.Maria.BeforeQuerys {
		maria := ConnectMaria()
		defer maria.Close()
		err := execQuery(maria, query)
		if err != nil {
			Error.Fatal(err.Error() + "," + query)
		}
		maria.Close()
	}

	// 마이그레이션
	for _, tableInfo := range conf.Tables {

		if strings.Contains(tableInfo.SourceName, "%") {

			// src 테이블이 복수개인 경우
			waitGlobal.Add(1)

			startTime := time.Now()

			var waitGroup sync.WaitGroup

			tableList := []Table{}
			reportList := []Report{}

			// 한번만 truncate 한다.
			if strings.EqualFold(tableInfo.BeforeTruncate, "true") {
				truncateTable(tableInfo.TargetName)
				Info.Printf("%s, truncate table", tableInfo.TargetName)
				tableInfo.BeforeTruncate = "false"
			}

			fmtString := tableInfo.SourceName

			for i := tableInfo.StartIndex; i <= tableInfo.EndIndex; i++ {

				tableInfo.SourceName = fmt.Sprintf(fmtString, i)
				if strings.Contains(tableInfo.SourceName, "!") {
					Error.Fatalf("Invalid SourceName: %s", fmtString)
				}

				tableList = append(tableList, tableInfo)

				waitGroup.Add(1)
				go func(tableInfo Table) {
					report := migrationTable(tableInfo)
					reportList = append(reportList, report)
					waitGroup.Done()
				}(tableInfo)
			}

			// group thread가 모두 끝나야 report를 한다.
			waitGroup.Wait()

			duration := time.Since(startTime)
			reportMultiTables(tableList, reportList, duration)

			waitGlobal.Done()

		} else {
			waitGlobal.Add(1)
			go func(tableInfo Table) {

				startTime := time.Now()

				report := migrationTable(tableInfo)

				duration := time.Since(startTime)
				reportTable(tableInfo, &report, duration)

				waitGlobal.Done()
			}(tableInfo)
		}
	}

	waitGlobal.Wait()

	// 마이그레이션후 쿼리
	for _, query := range conf.Maria.AfterQuerys {
		maria := ConnectMaria()
		defer maria.Close()
		err := execQuery(maria, query)
		if err != nil {
			Error.Fatal(err.Error() + "," + query)
		}
		Info.Printf("%s", query)
	}

	duration := time.Since(startTime)
	Info.Printf("%d Tables Duration %v", len(conf.Tables), duration)

	Info.Println("End Job.")
}
